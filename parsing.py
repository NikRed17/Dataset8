import os
import time
import random
import uuid
import sqlite3
import requests
import concurrent.futures
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Настройки ===
SITEMAP_URL = "https://www.newsvl.ru/sitemap_vl_news.xml"
DB_FILE = "vladivostok_news2.db"
MAX_ARTICLES = 5000  # Целевое количество статей
MAX_WORKERS = 5  # Количество потоков для параллельного парсинга

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 YaBrowser/25.6.1.1000 Yowser/2.5 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
]

REQUEST_DELAY = (0.3, 0.8)  # Оптимизировано для скорости
RETRY_ATTEMPTS = 3


# === 1. Создаём базу данных с оптимизацией ===
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    # Удаляем старую таблицу если существует (для чистого запуска)
    cur.execute("DROP TABLE IF EXISTS articles")

    cur.execute("""
        CREATE TABLE articles (
            guid TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            url TEXT NOT NULL UNIQUE,
            published_at TEXT,
            comments_count INTEGER DEFAULT 0,
            created_at_utc TEXT NOT NULL,
            rating INTEGER,
            word_count INTEGER GENERATED ALWAYS AS (LENGTH(description) - LENGTH(REPLACE(description, ' ', '')) + 1) VIRTUAL
        )
    """)

    # Создаем индексы для быстрой работы
    cur.execute("CREATE INDEX idx_url ON articles(url)")
    cur.execute("CREATE INDEX idx_published ON articles(published_at)")
    cur.execute("CREATE INDEX idx_created ON articles(created_at_utc)")

    conn.commit()
    conn.close()
    print(f"База данных '{DB_FILE}' создана с индексами")


# === 2. Улучшенная сессия requests с повторами ===
def create_session():
    session = requests.Session()

    # Настраивает программу для загрузки страниц. Делаем вежливым,
    # Настраиваем повторные попытки, если сайт недоступен и используем разные User Agent, чтобы не заблокали
    retry_strategy = Retry(
        total=RETRY_ATTEMPTS,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=20, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


# === 3. Получаем URL из sitemap с фильтрацией ===
# === С помощью sitemap.xml загружает оглавление сайта ===
def fetch_sitemap_urls(session, limit=7000):
    """Получаем URL из sitemap"""
    print("Загружаем sitemap...")
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    try:
        resp = session.get(SITEMAP_URL, headers=headers, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"Ошибка загрузки sitemap: {e}")
        return []

    root = ET.fromstring(resp.content)
    urls_with_dates = []

    # Собираем URL и даты для сортировки
    for url_tag in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}url"):
        loc = url_tag.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
        lastmod = url_tag.find("{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod")

        if loc is not None:
            url = loc.text.strip()
            date = lastmod.text if lastmod is not None else ""
            urls_with_dates.append((url, date))

    print(f"Найдено {len(urls_with_dates)} записей в sitemap")

    # Сортируем по дате (свежие сначала) и берем нужное количество
    urls_with_dates.sort(key=lambda x: x[1], reverse=True)
    urls = [url for url, date in urls_with_dates[:limit]]

    print(f"Отобрано {len(urls)} самых свежих URL")
    return urls


# === 4. Очистка текста ===
def clean_article_text(soup):
    """Извлекаем и очищаем текст статьи"""
    # Основной текст
    text_block = soup.find("div", class_="story__text")
    if not text_block:
        # Пробуем альтернативные селекторы
        text_block = soup.find("div", class_=lambda x: x and ("article" in x or "content" in x or "text" in x))
        if not text_block:
            return None

    # Клонируем блок для безопасного удаления элементов
    text_block = BeautifulSoup(str(text_block), 'html.parser')

    # Удаляем нежелательные элементы
    unwanted_selectors = [
        "img", "video", "audio", "iframe", "script", "style",
        "figure", ".embed-responsive", ".social-share", ".advertisement",
        ".banner", "ins", ".ya-share2", ".teaser", "[data-type='ad']"
    ]

    for selector in unwanted_selectors:
        for element in text_block.select(selector):
            element.decompose()

    # Убираем ссылки, но сохраняем текст
    for a in text_block.find_all("a"):
        a.replace_with(a.get_text())

    # Убираем пустые теги
    for tag in text_block.find_all():
        if not tag.get_text(strip=True) and not tag.attrs:
            tag.decompose()

    # Получаем чистый текст
    text = text_block.get_text(separator="\n", strip=True)

    # Очистка от лишних пробелов и пустых строк
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    cleaned_text = "\n".join(lines)

    # Проверяем минимальную длину
    if len(cleaned_text) < 300:  # Минимум 300 символов
        return None

    return cleaned_text


# === 5. Парсим одну статью ===
def parse_article(url, session):
    """Парсим статью с обработкой ошибок"""
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    try:
        resp = session.get(url, headers=headers, timeout=10)

        if resp.status_code != 200:
            if resp.status_code == 404:
                return {"status": "skipped", "reason": "404 Not Found"}
            elif resp.status_code == 403:
                return {"status": "error", "reason": "403 Forbidden"}
            else:
                return {"status": "error", "reason": f"HTTP {resp.status_code}"}

        # Проверяем что это HTML страница
        if 'text/html' not in resp.headers.get('content-type', '').lower():
            return {"status": "skipped", "reason": "Not HTML"}

        soup = BeautifulSoup(resp.content, "html.parser", from_encoding='utf-8')

        # Заголовок
        title_elem = soup.find("h1", class_="story__title")
        if not title_elem:
            title_elem = soup.find("h1")

        title = title_elem.get_text(strip=True) if title_elem else "Без заголовка"

        # Текст статьи
        description = clean_article_text(soup)
        if not description:
            return {"status": "skipped", "reason": "No text content"}

        # Дата публикации
        published_at = None

        # Пробуем найти дату в мета-тегах
        meta_date = soup.find("meta", property="article:published_time") or \
                    soup.find("meta", property="og:published_time") or \
                    soup.find("meta", attrs={"name": "pubdate"})

        if meta_date and meta_date.get("content"):
            published_at = meta_date["content"]
        else:
            # Извлекаем из URL
            import re
            match = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", url)
            if match:
                year, month, day = match.groups()
                published_at = f"{year}-{month}-{day} 00:00:00"
            else:
                # Ищем дату в тексте
                date_patterns = [
                    r'\d{1,2}\s+[а-я]+\s+\d{4}',
                    r'\d{1,2}\.\d{1,2}\.\d{4}'
                ]
                for pattern in date_patterns:
                    match = re.search(pattern, soup.get_text()[:500])
                    if match:
                        published_at = match.group(0)
                        break

        # Комментарии
        comments_count = 0
        comments_elem = soup.find(class_=lambda x: x and ("comment" in x.lower() or "коммент" in x.lower()))
        if comments_elem:
            import re
            numbers = re.findall(r'\d+', comments_elem.get_text())
            if numbers:
                comments_count = int(numbers[0])

        return {
            "status": "success",
            "data": {
                "guid": str(uuid.uuid4()),
                "title": title[:500],  # Ограничиваем длину
                "description": description,
                "url": url,
                "published_at": published_at,
                "comments_count": comments_count,
                "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "rating": None
            }
        }

    except requests.exceptions.Timeout:
        return {"status": "error", "reason": "Timeout"}
    except requests.exceptions.RequestException as e:
        return {"status": "error", "reason": str(e)}
    except Exception as e:
        return {"status": "error", "reason": f"Parse error: {str(e)}"}


# === 6. Пакетное сохранение в БД  ===
class DatabaseBatchSaver:
    def __init__(self, db_file, batch_size=100):
        self.db_file = db_file
        self.batch_size = batch_size
        self.batch = []
        self.total_saved = 0

    def add_article(self, article):
        self.batch.append(article)
        if len(self.batch) >= self.batch_size:
            self.flush()

    def flush(self):
        if not self.batch:
            return

        conn = sqlite3.connect(self.db_file)
        cur = conn.cursor()

        try:
            cur.executemany("""
                INSERT OR IGNORE INTO articles
                (guid, title, description, url, published_at, comments_count, created_at_utc, rating)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                (
                    article["guid"],
                    article["title"],
                    article["description"],
                    article["url"],
                    article["published_at"],
                    article["comments_count"],
                    article["created_at_utc"],
                    article["rating"]
                )
                for article in self.batch
            ])

            saved_count = cur.rowcount
            self.total_saved += saved_count
            conn.commit()

            print(f"Сохранено пакетом: {saved_count} статей (всего: {self.total_saved})")

        except Exception as e:
            print(f"Ошибка при пакетном сохранении: {e}")
            # Сохраняем по одной при ошибке
            for article in self.batch:
                try:
                    cur.execute("""
                        INSERT OR IGNORE INTO articles
                        (guid, title, description, url, published_at, comments_count, created_at_utc, rating)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, tuple(article.values()))
                except:
                    continue
            conn.commit()
        finally:
            conn.close()

        self.batch.clear()

    def close(self):
        self.flush()
        return self.total_saved


# === 7. Параллельный парсинг статей ===
def parse_articles_parallel(urls, max_articles=MAX_ARTICLES):
    """Парсим статьи в несколько потоков"""
    print(f"Запуск параллельного парсинга ({MAX_WORKERS} потоков)...")

    db_saver = DatabaseBatchSaver(DB_FILE, batch_size=50)
    session = create_session()

    stats = {
        "total": 0,
        "success": 0,
        "skipped": 0,
        "errors": 0,
        "start_time": time.time()
    }

    def process_url(url):
        nonlocal stats
        stats["total"] += 1

        # Периодически показываем прогресс
        if stats["total"] % 100 == 0:
            elapsed = time.time() - stats["start_time"]
            speed = stats["total"] / elapsed if elapsed > 0 else 0
            print(f"Прогресс: {stats['total']} | Успешно: {stats['success']} | "
                  f"Скорость: {speed:.1f} статей/сек | "
                  f"Время: {elapsed / 60:.1f} мин")

        # Задержка для избежания блокировки
        time.sleep(random.uniform(*REQUEST_DELAY))

        result = parse_article(url, session)

        if result["status"] == "success":
            db_saver.add_article(result["data"])
            stats["success"] += 1
            return "success"
        elif result["status"] == "skipped":
            stats["skipped"] += 1
            return "skipped"
        else:
            stats["errors"] += 1
            return "error"

    # Ограничиваем количество URL для парсинга
    urls_to_parse = urls[:max_articles * 2]  # Берем с запасом

    # Используем ThreadPoolExecutor для параллельного парсинга
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_url, url): url for url in urls_to_parse}

        completed = 0
        for future in concurrent.futures.as_completed(futures):
            completed += 1

            # Останавливаемся если собрали достаточно статей
            if stats["success"] >= max_articles:
                print(f"Достигнута цель: {max_articles} статей!")
                executor.shutdown(wait=False, cancel_futures=True)
                break

            # Прогресс каждые 50 статей
            if completed % 50 == 0:
                print(f"Обработано: {completed}/{len(urls_to_parse)} | "
                      f"Собрано: {stats['success']}/{max_articles}")

    # Сохраняем оставшиеся статьи
    total_saved = db_saver.close()

    # Выводим статистику
    elapsed = time.time() - stats["start_time"]
    print(f"\n{'=' * 60}")
    print("СТАТИСТИКА ПАРСИНГА")
    print(f"{'=' * 60}")
    print(f"Успешно сохранено: {total_saved} статей")
    print(f"Пропущено: {stats['skipped']} статей")
    print(f"Ошибок: {stats['errors']}")
    print(f"Общее время: {elapsed / 60:.1f} минут")
    print(f"Скорость: {stats['total'] / elapsed:.1f} статей/сек")
    print(f"{'=' * 60}")

    session.close()
    return total_saved


# === 8. Проверка базы данных ===
def check_database():
    """Проверяем и показываем содержимое базы"""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    # Общая информация
    cur.execute("SELECT COUNT(*) FROM articles")
    total = cur.fetchone()[0]

    cur.execute("""
        SELECT 
            MIN(published_at) as earliest,
            MAX(published_at) as latest,
            COUNT(DISTINCT DATE(published_at)) as days_count
        FROM articles 
        WHERE published_at IS NOT NULL
    """)

    stats = cur.fetchone()

    cur.execute("""
        SELECT title, url, published_at, LENGTH(description) as length
        FROM articles 
        ORDER BY RANDOM() 
        LIMIT 3
    """)

    samples = cur.fetchall()

    conn.close()

    print(f"\nПРОВЕРКА БАЗЫ ДАННЫХ:")
    print(f"   Всего статей: {total}")
    if stats[0]:
        print(f"   Первая статья: {stats[0]}")
        print(f"   Последняя статья: {stats[1]}")
        print(f"   Статей за дней: {stats[2]}")

    print(f"\nПримеры статей:")
    for i, (title, url, date, length) in enumerate(samples, 1):
        print(f"   {i}. {title[:60]}...")
        print(f"      URL: {url[:50]}...")
        print(f"      Дата: {date[:10] if date else 'Нет'}")
        print(f"      Длина: {length} символов")

    # Проверяем размер файла
    if os.path.exists(DB_FILE):
        size_mb = os.path.getsize(DB_FILE) / (1024 * 1024)
        print(f"\n Размер файла БД: {size_mb:.2f} MB")

    return total


# === 9. Основной запуск ===
def main():
    print("=" * 60)
    print("ПАРСЕР VLADIVOSTOK NEWS")
    print("=" * 60)

    # Инициализация БД
    init_db()

    # Создаем сессию для загрузки sitemap
    session = create_session()

    # Получаем URL
    urls = fetch_sitemap_urls(session, limit=7000)

    if not urls:
        print("Не удалось загрузить URL. Завершение работы.")
        return

    print(f"Цель: собрать {MAX_ARTICLES} статей")
    print(f"Доступно URL: {len(urls)}")

    # Закрываем сессию sitemap
    session.close()

    # Запускаем параллельный парсинг
    total_saved = parse_articles_parallel(urls, MAX_ARTICLES)

    # Проверяем результат
    check_database()

    if total_saved >= MAX_ARTICLES:
        print(f"\n УСПЕХ! Собрано {total_saved} статей (цель: {MAX_ARTICLES})")
        print(f"Файл базы данных: {DB_FILE}")
    else:
        print(f"\n Собрано только {total_saved} статей из {MAX_ARTICLES}")
        print("   Попробуйте увеличить MAX_WORKERS или уменьшить REQUEST_DELAY")


# === 10. Быстрый тест ===
def quick_test():
    """Быстрая проверка работы парсера"""
    print("🔧 ТЕСТОВЫЙ РЕЖИМ (соберет 20 статей)")

    global MAX_ARTICLES
    MAX_ARTICLES = 20

    init_db()

    session = create_session()
    urls = fetch_sitemap_urls(session, limit=50)
    session.close()

    if urls:
        # Парсим последовательно для теста
        db_saver = DatabaseBatchSaver(DB_FILE, batch_size=10)

        for i, url in enumerate(urls[:30], 1):
            if db_saver.total_saved >= 20:
                break

            print(f"[{i}] Тест: {url[:60]}...")
            result = parse_article(url, create_session())

            if result["status"] == "success":
                db_saver.add_article(result["data"])
                print(f"   Успех")
            else:
                print(f"   Пропущено: {result['reason']}")

            time.sleep(0.5)

        db_saver.close()
        check_database()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        quick_test()
    else:
        main()