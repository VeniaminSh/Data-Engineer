## Проект
Описание проекта: проект состоит из модулей, каждый модуль разбит на задачи. Каждая задача формировалась самостоятельно, детально прописаны пути решения.

Решения, которые были использованы для проектирования задач.

### Модуль 1. Составить четыре аналитических дашборда в Excel:

1) Продажи менеджеров
2) Динамика прибыли по месяцам
3) Фин показатели по категориям
4) Картограмма 
---
На вход даны 3 csv файла: 

[orders - заказы](https://disk.yandex.ru/d/w0MxjkXIMkpaRg), 

[peple - менеджеры](https://disk.yandex.ru/d/MmyCjTltpzK8Hg), 

[returns - возвраты](https://disk.yandex.ru/d/ihMjWUkE2vLv9g)

---
#### Шаг 1.
Для загрузки csv в excel воспользовались надсткройкой power query. Данные с csv в файл xlsx будем тянуть прямоком из яндекс диска (см. данные -> запросы и подклбчения -> первый примененый шаг (источник))

```= Csv.Document(Web.Contents("ссылка на файл"),[Delimiter=";", Columns=21, Encoding=65001, QuoteStyle=QuoteStyle.None])```

В power query произведена небольшая предобработка данных всех трех таблиц. 

#### Шаг 2.
Составление содержания книги xlsx - определение ссылок на листы описание полей таблиц источников.  

#### Шаг 3.
Для составления дашборда "Продажи менеджеров", сформировали из подключений модели power pivot и соединили два источника orders и people по ключу region. Сам дашборд состоит из агрегата продаж и выручки по менеджерам и фильров по годам и месяцам , так же для наглядности выведен дашборд. 

"Динамика прибыли по месяцам" - сводная таблица из модели таблицы orders, выведена динамика продаж и прибыли по годам и месяцам.

"Категории" - выведены категории и подкатегории товаров с прибылью и продажами, так же обозначены фильтры на год и штат. 

"Картограмма" - на данном дашборде с помощью формул, тк график карты не работает с данными из сводных таблиц, выведены данные , на основе которых строится карта с продажами по штатам.

Для следующей задачи подготовлен пустой лист Coordinates.
Итоговый файл: [тут](https://disk.yandex.ru/d/I-LJOHqSt3EjFQ)



### Модуль 2. Как донести данные до бизнес-пользователя с помощью Data Lens

По итогам 1 модуля, мы получили файл, который обновляется по запросу, обновить данные из источника можно слудющим образом (открыть excel в вкладке **Данные** кликнуть по кнопке **Обновить все**) обновленные данные подтянутся с диска. 

Или можно автоматизировать данную задачу на основе python 

```
# импорт библиотек
import win32com.client
import time

#обозначаем путь откуда будут тянуться данные
path = "C:\\Users\\Artem_Amira\\Desktop\\Аналитик данных + Инженер\\Дата инженер\\Data-Engineer\\Excel - Pivot - Query - Python\\Sample.xlsx"

def update excel():
    # Запускаем Excel
    xlapp = win32com.client.DispatchEx("Excel.Application")

    # Вывод сообщений и оповещений во время выполнения 
    xlapp.DisplayAlerts = True #True

    #Вывод листа на экран
    xlapp.Visible = True
    
    #запускаем книгу 
    wb = xlapp.workbooks.open(path)

    #ждем 10 сек пока запустится и обновляем данные
    time.sleep(10)
    wb.RefreshAll()

    #ждем пока не закончится обновление
    xlapp.CalculateUntilAsyncQueriesDone()

    #сохраняем файл и закрываем с уведомлением о сохарнении
    wb.Save()
    time.sleep(10)
    wb.Close(SaveChanges=True)

    #спим и закрываем экземпляр Excel
    time.sleep(10)
    xlapp.Quit()

    #удаляем экземпляр Excel
    del xlapp

update excel(path)
```

Исходя из данных, которые есть, я решил вывести 

* 4 индикатора - агрегированные общие показатели: продаж, кол-ва проданного товара, суммы скидок в валютной единице, общая доля прибыли от продаж

* 3 дашборда - Продажи и прибыль по менеджерам, Топ 5 городов по прибыли, Прибыль в сравении с количеством проданных товаров.

* карту - Отношение маржи к прибыли по городам. 

* дерево - кол-во проданных подкатегорий

Исходными данными для построения дашборда выступает лист Orders из книги Sample. Для построения карты нам необходимы координаты городов, мы достанем их с помощью библиотеки ```geopy``` из python.

```
#импортируем библиотеки
from geopy.geocoders import Nominatim
import pandas as pd
from datetime import datetime
import win32com.client
import time
```

```
#ссылка на библиотеку
path = 'C:\\Users\\Artem_Amira\\Desktop\\Аналитик данных + Инженер\\Дата инженер\\Data-Engineer\\Excel - Pivot - Query - Python\\Sample.xlsx'

#составим функцию, которая достанет точки координат по названию страны, штату и городу

def create_coordinate(path):
    #открываем excel файл
    df = pd.read_excel(path)
    #развернем сервис геолокации
    locator = Nominatim(user_agent = "myapp")

    #создаем уникальные пары по шаблону "Страна, Штат, Город" для формирования справочника с координатами
    unique = (df['Country'] + ',' + df['State'] + ',' + df['City']).unique()
    
    #применяем наш сервис к уникальным парам и добавляем к строке само уникальное значение
    city_to_coord = list(zip(unique, [locator.geocode(city) for city in unique]))

    #создаем пустой датафрейм, куда будем складывать обработанные значения
    coord = pd.DataFrame(index = None)
    
    #место для приемки пустых значений
    coordinate_list = []
    city_list = []
    state_list = []

    #вытаскиваем долготу и широту, город и штат и складываем в переменные 
    for x in range(len(city_to_coord)):
        coordinate = (str(city_to_coord[x][1].latitude) + ", " + str(city_to_coord[x][1].longitude))
        coordinate_list.append(coordinate)
        city_list.append([(x.split(',')[2]) for x in city_to_coord[x][:1]])  #city_to_coord[x].address.split(",")[0]
        state_list.append([x.split(',')[1] for x in city_to_coord[x][:1]])
    
    #насыщаем датафрейм даными 
    coord['Coordinates'] = ['[' + x + ']' for x in coordinate_list]
    coord['City'] = [x[0] for x in city_list]
    coord['State'] = [x[0] for x in state_list]

create_coordinate(path) 
```

Напишем вторую функцию, которая будет открывать xlmx файл и заполнять лист Coordinates данными, которые мы получили в фукции ```create_coordinate``` это поля с координатами, городами и штатами.

```
def write(path):
    # Запускаем экземпляр Excel
    xlapp = win32com.client.DispatchEx("Excel.Application")

    # Вывод сообщений и оповещений во время выполнения 
    xlapp.DisplayAlerts = True #True

    #Вывод листа на экран
    xlapp.Visible = True
    #открываем excel файл
    wb = xlapp.workbooks.open(path)

    time.sleep(10)
    worksheet = wb.Worksheets('Coordinates')

    # Определяем начальную ячейку для вставки данных (например, A1)
    start_row = 1
    start_col = 1

    #цикл, который заполняет лист
    for i in range(coord.shape[0]):
        for j in range(coord.shape[1]):
            worksheet.Cells(start_row + i, start_col + j).Value = coord.iat[i, j]

    #сохраняем файл и закрываем с уведомлением о сохарнении
    wb.Save()
    time.sleep(10)
    wb.Close(SaveChanges=True)

    #спим и закрываем экземпляр Excel
    time.sleep(10)
    xlapp.Quit()

    #удаляем экземпляр Excel
    del xlapp 
```
Почему я не выбрал библиотеку ```openpyxl```, потому что при открытии питоном файла ексель, все сводные таблицы ломаются.

В самом екселе протянули формулу на листе orders ```индекс(поискпоз)```, которая обращается к нашему справочнику coordinates и проставляет координаты в новый столбец.

[Итоговый файл](https://disk.yandex.ru/i/YflSdWyCEkciRQ), к которому обращается yandex data lens.

Сам [дашборд](https://datalens.yandex/xsfxg3adqip0k) выглядит следующим образом.

