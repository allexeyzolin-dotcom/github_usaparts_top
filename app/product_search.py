"""Product search copy derived only from verified catalog fields."""
import re
import unicodedata

MAX_QUERIES = 6

# Literal naming equivalents only: preserve model, side, year and part numbers.
# These are search variants, not claims about fitment or measured search volume.
UKRAINIAN_TERMS = {
    "топливный": "паливний", "топливного": "паливного", "топливная": "паливна",
    "высокого давления": "високого тиску", "низкого давления": "низького тиску",
    "поддон": "піддон", "крыло": "крило", "крыла": "крила", "крылья": "крила",
    "крышка": "кришка", "крышки": "кришки", "крепление": "кріплення",
    "зеркало": "дзеркало", "зеркала": "дзеркала", "фонарь": "ліхтар", "фонаря": "ліхтаря",
    "радиатор": "радіатор", "радиатора": "радіатора", "вентилятор": "вентилятор",
    "тормозной": "гальмівний", "тормозные": "гальмівні", "колодки": "колодки",
    "рычаг": "важіль", "рычага": "важеля", "подвески": "підвіски",
    "двигатель": "двигун", "двигателя": "двигуна", "двигателю": "двигуна",
    "передний": "передній", "передняя": "передня", "переднее": "переднє",
    "задний": "задній", "задняя": "задня", "заднее": "заднє",
    "правый": "правий", "правая": "права", "правое": "праве",
    "левый": "лівий", "левая": "ліва", "левое": "ліве",
    "передних": "передніх", "задних": "задніх", "охлаждения": "охолодження",
    "масляный": "оливний", "масляного": "оливного", "фильтр": "фільтр",
    "проводка": "проводка", "стекло": "скло", "стекла": "скла",
    "зажигания": "запалювання", "рулевой": "кермовий", "рулевая": "кермова",
    "коробка передач": "коробка передач", "уплотнитель": "ущільнювач",
    "шланг": "шланг", "бачок": "бачок", "ремень": "ремінь", "решетка": "решітка",
    "решётка": "решітка", "безопасности": "безпеки", "сиденье": "сидіння",
}
UKRAINIAN_PATTERN = re.compile(r"\b(?:" + "|".join(re.escape(term) for term in
    sorted(UKRAINIAN_TERMS, key=len, reverse=True)) + r")\b", re.I)


def ukrainian_variant(name):
    return UKRAINIAN_PATTERN.sub(lambda match: UKRAINIAN_TERMS[match.group().casefold()], name)


def clean_text(value):
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = re.sub(r"<[^>]*>", " ", text)
    text = re.sub(r"[\x00-\x1f\x7f]", " ", text)
    return " ".join(text.split())


def shorten(value, limit):
    text = clean_text(value)
    if len(text) <= limit:
        return text
    return text[:limit - 1].rsplit(" ", 1)[0].rstrip(" ,.;:-") + "…"


def query_list(value):
    values = re.split(r"[\n;]+", value) if isinstance(value, str) else (value or [])
    result, seen = [], set()
    for value in values:
        text = shorten(value, 120)
        key = text.casefold()
        if len(text) >= 3 and key not in seen:
            seen.add(key)
            result.append(text)
        if len(result) == MAX_QUERIES:
            break
    return result


def generate_product_search(*, name, number, brand="", condition="", available=False, cross_numbers=()):
    number = clean_text(number)
    name = clean_text(name)
    if number:
        name = re.sub(r"^" + re.escape(number) + r"(?:\s*[-–—:]\s*|\s+|$)", "", name, flags=re.I)
    name = name or "Запчастина"
    brand = clean_text(brand)
    if brand.casefold() in {"oem", "оригінал", "аналог", "замінник", "usaparts.top", "usa auto parts", "-"}:
        brand = ""
    subject = name
    if brand and brand.casefold() not in name.casefold():
        subject += " " + brand
    # Reserve room for the part code even when the imported name is very long.
    search_subject = shorten(subject, max(35, 115 - len(number)))
    identity = " ".join(item for item in (search_subject, number) if item)
    queries = [identity]
    translated = ukrainian_variant(search_subject)
    if translated.casefold() != search_subject.casefold():
        queries.append(f"{translated} {number}".strip())
    queries.extend([" ".join(item for item in (number, brand) if item), search_subject])
    compact_number = re.sub(r"[\s\-_.]+", "", number)
    if compact_number and compact_number != number:
        queries.append(f"{search_subject} {compact_number}")
    queries.append(f"купити {identity}")
    # Cross numbers come from the master card, never inferred compatibility.
    queries.extend(f"{search_subject} {clean_text(cross)}" for cross in list(cross_numbers)[:2])
    title_code = shorten(number, 40)
    title = " ".join(item for item in (shorten(subject, max(20, 72 - len(title_code) - 1)), title_code) if item) + " | USAparts.top"
    sentences = [f"{identity}."]
    if condition:
        sentences.append(f"Стан: {clean_text(condition).lower()}.")
    sentences.append("Є в наявності." if available else "Наразі немає в наявності.")
    sentences.append("Перевірте сумісність за артикулом або VIN у продавця.")
    return {"title": title, "description": shorten(" ".join(sentences), 160),
            "queries": query_list(queries), "summary": " ".join(sentences)}
