import os, json, random
from datetime import date, timedelta

# Настройки
num_tours = 30
num_tourists = 30
start_date = date(2020, 1, 1)

# Данные
real_countries = ["Россия", "Италия", "Испания", "Франция", "Таиланд"]
funny_places = ["в ад", "на Луну", "в Бобруйск", "на дачу к теще", "в офис", "в трясину", "в Зазеркалье"]
real_places = ["Барселону", "Милан", "Бангкок", "Париж", "Сочи", "Рим", "Прагу", "Амстердам"]

funny_adjectives = ["Последнее", "Безумное", "Забытое", "Бессмысленное", "Потустороннее", "Потное"]
real_adjectives = ["Увлекательное", "Экзотическое", "Романтическое", "Приключенческое", "Историческое", "Расслабляющее"]
trip_types = ["путешествие", "тур", "поездка"]

first_names = ["Алексей", "Мария", "Иван", "Ольга", "Сергей", "Анна", "Дмитрий", "Елена", "Николай", "Татьяна"]
last_names = ["Иванов", "Смирнова", "Кузнецов", "Попова", "Соколов", "Морозова", "Лебедев", "Козлова", "Новиков", "Федорова"]

positive_reviews = ["Отличный тур, рекомендую!", "Все понравилось!", "Незабываемое путешествие!", "Очень доволен поездкой."]
neutral_reviews = ["В целом нормально.", "Было интересно, но ожидал большего.", "Средний тур."]
negative_reviews = ["Разочарован, не оправдало ожиданий.", "Сервис оставляет желать лучшего.", "Больше не поеду."]

def generate_description():
    intro_phrases = [
        "Для любителей острых ощущений",
        "Идеально подходит тем, кто устал от обычного",
        "Уникальный шанс испытать нечто новое",
        "Для тех, кто хочет убежать от рутины",
        "Подарите себе незабываемые эмоции",
        "Предложение для настоящих романтиков",
        "Когда отпуск зовёт приключения",
    ]

    actions = [
        "экскурсия по",
        "плавание в",
        "пеший поход через",
        "медитация среди",
        "ночёвка на вершине",
        "спонтанный квест в",
        "прогулка с гидами по",
        "неожиданные встречи в",
    ]

    locations_real = [
        "древним улочкам Рима",
        "живописным долинам Франции",
        "пляжам Таиланда",
        "набережной Барселоны",
        "лесам Карелии",
        "улицам старого города",
    ]

    locations_funny = [
        "переулкам Зазеркалья",
        "заброшенным пивоварням",
        "котокафе на краю вселенной",
        "мысу Отчаяния",
        "тёмным лесам бюрократии",
        "кладбищу надежд и Wi-Fi",
    ]

    endings = [
        "Включено: трансфер, питание и пара экзистенциальных кризисов.",
        "Никакой скуки — только вы, природа и немного паники.",
        "Гарантируем: вернётесь другим человеком. Возможно.",
        "Без лишних слов — просто берите билет.",
        "Подходит даже тем, кто боится приключений.",
        "Берите зонт. Или зелье от страха.",
    ]

    # Случайно миксуем реальные и юмористические локации
    location = random.choice(locations_real + locations_funny)

    return f"{random.choice(intro_phrases)}, {random.choice(actions)} {location}. {random.choice(endings)}"


# Папки
os.makedirs('data/tours', exist_ok=True)
os.makedirs('data/tourists', exist_ok=True)

# Генерация туров
for i in range(1,num_tours+1):
    adj = random.choice(real_adjectives + funny_adjectives)
    place = random.choice(real_places + funny_places)
    name = f"{adj} {random.choice(trip_types)} {place}"
    tour = {
        'id_тура': i,
        'название': name,
        'страна': random.choice(real_countries),
        'описание': generate_description(),
        'стоимость': round(random.uniform(50000, 200000), 2),
        'услуга': random.sample(["экскурсия", "трансфер", "питание", "страховка"], random.randint(1, 4))
    }
    with open(f"data/tours/tour_{i}.json", 'w', encoding='utf-8') as f:
        json.dump(tour, f, ensure_ascii=False)

# Генерация туристов и покупок
for i in range(1,num_tourists+1):
    fio = f"{random.choice(last_names)} {random.choice(first_names)}"
    purchase_date = start_date + timedelta(days=random.randint(0, 1000))
    tour_id = random.randint(1, num_tours)
    review_pool = random.choices([positive_reviews, neutral_reviews, negative_reviews], weights=[0.6, 0.25, 0.15])[0]
    review = random.choice(review_pool)

    tourist = {
        'id_туриста': i,
        'персональные_данные': fio,
        'дата_тура': purchase_date.isoformat(),
        'id_тура': tour_id,
        'страна': random.choice(real_countries),
        'сведения_о_визе': "Тип визы и дата выдачи.",
        'отзыв': review
    }
    with open(f"data/tourists/tourist_{i}.json", 'w', encoding='utf-8') as f:
        json.dump(tourist, f, ensure_ascii=False)
