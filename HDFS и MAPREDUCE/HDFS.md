Суть данного тест проекта, поднять HDFS кластер на Yandex Cloud, загрузить файлы из нескольких систем и выполнить простое mapreduce задание.

# Создаем Hadoop кластер в Yandex Cloude

В яндексе hadoop называется Data Proc. После выбора данного инструмента в панели каталога.
Указываем настройки кластера. 
* Имя кластера: hdfs-lab
* Версия 1.4 (у Hadoop это версия 2)
* Сервисы: HDFS, YARN, MAPREDUCE, SPARK, HIVE
* Необхожимо создать [ssh ключ](https://git-scm.com/book/ru/v2/Git-%D0%BD%D0%B0-%D1%81%D0%B5%D1%80%D0%B2%D0%B5%D1%80%D0%B5-%D0%93%D0%B5%D0%BD%D0%B5%D1%80%D0%B0%D1%86%D0%B8%D1%8F-%D0%BE%D1%82%D0%BA%D1%80%D1%8B%D1%82%D0%BE%D0%B3%D0%BE-SSH-%D0%BA%D0%BB%D1%8E%D1%87%D0%B0), затем ```ls ~/.ssh/``` - посмотрим последний сгененерированный ключ с форматом ```.pub``` и скопируем его значение ```cat ~/.ssh/id_rsa.pub | clip``` и вставим в поле SSH-ключ 
* выбрать сервисный акк, если нет создать
* любую зону доступности 
* необходимо создать сеть 
    * первым действием необходимо установить [Yandex Cloud (CLI)](https://yandex.cloud/ru/docs/cli/quickstart#install) ПО для управления облачными ресурсами через cmd. По это инструкции мы сдедали профиль, как итог - при выводе команды ```yc config list``` должен быть результат с названием токена, id клауда, id папки и зоны сервера. 
    * вторым действием настраиваем [сеть](https://yandex.cloud/ru/docs/vpc/operations/create-nat-gateway) , в 5 пункте подсеть создавать необязательно, YC создает 3 подсети самостоятельно, достаточно взять имя той, которая совпадает с выбраной зоной доступности (найти можно в сервисах каталога).
* выбираем UI Proxy - необходима для доступа к веб-интерфейсам компонентов кластера через консоль управления.
* Подкластеры - мастер нода = Name Noda, выбираем одну машину на 16 ГБ, Дата noda, выберем две машины  по 16 ГБ. Создаем кластер.


Hadoop на YC поднят.  

Для лакальной работы с YDP необходимо установить публичную сеть для всех наших нод Хосты - > выбор необходимой ВМ - > графа сеть - > + публичный адрес.



### Формирование и распределeние файлов на HDFS
Подключиться к мастер ноде через ssh.
```ssh root@<публичный адрес>```

Создадим файл и положим его в hdfs и посмотри содержимое 
```echo "Hi my name Veniamin" > ven.md ; hadoop fs -put *.md ; hadoop fs -text /user/root/ven.md```

Посмотрим отчет по файлу, который сделали, куда реплицируется и тд.
```hdfs fsck ven.md -files -files -blocks -locations```

Имя файла - blk_1073741837

т.к. Default replication factor: 1, т.е. файл не реплицируется
Сделаем репликацию: ```hadoop fs -setrep 2 ven.md```.

```hdfs fsck ven.md -files -files -blocks -locations``` после перепроверки реплика перенеслась на один из серверов, зайдем на него по публичному адресу, директория с файлом хранится в пути /hadoop/dfs/data/current ```cd /hadoop/dfs/data/current ; ls```.

Найдем файл по имени ```find . -name 'blk_1073741837'```. Результат вывода, можно посомотреть ```cat ./BP-1035751435-10.128.0.31-1722669702662/current/finalized/subdir0/subdir0/blk_1073741837```.

Так же, при удалении файла с дата ноды, он реплицируется на другую дата ноду.
```rm ./BP-1035751435-10.128.0.31-1722669702662/current/finalized/subdir0/subdir0/blk_1073741837```


## MapReduce
Практика - выполнить wordcount в текстовом файле. На вход даны два файла 
из открытой библиотек Гутенберга - Алиса в стране чудес и Франкинштейн.

### Шаг. 1 Проводем подобную работу с файлами, которые нужно скачать из открытого доступа

Скачиваем данные для теста
```wget -O alice.txt https://www.gutenberg.org/files/11/11-0.txt```
```wget -O frank.txt https://www.gutenberg.org/files/84/84-0.txt```

Командой ls можно проверить, что файлы на месте. Данные файлы хранятся на локальной мастер ноде. Теперь их нужно переместить на hdfs. Для начала создадим папку, где эти файлы буду храниться.
```hadoop fs -mkdir -p /user/root/input-data```

Скопируем файлы, в эту папку.
```hadoop fs -put *.txt input-data```

### Шаг. 2 Выполнение задания

Для начала необходимо создать папку для скриптов map и reduce на мастер ноде
```mkdir /tmp/mapreduce```

Далее необходимо перенести фацлы с локально метахранения на мастер ноду по публичному адресу мастер ноды
```scp ./*.py root@89.169.143.53:/tmp/mapreduce/ ; scp ./run.sh root@89.169.143.53:/tmp/mapreduce/```

Перед запуском MapReduce задания необходимо обозначить переменную MR_OUTPUT
```export MR_OUTPUT=/user/root/output-data```

Ну и запускаем само задание

```
hadoop jar "$HADOOP_MAPRED_HOME"/hadoop-streaming.jar \
-Dmapred.job.name='Simple streaming job reduce' \
-file /tmp/mapreduce/mapper.py -mapper /tmp/mapreduce/mapper.py \
-file /tmp/mapreduce/reducer.py -reducer /tmp/mapreduce/reducer.py \
-input /user/root/input-data -output $MR_OUTPUT
```



# Интеграция s3 и Hadoop
Переходим на дата ноду. 

Необходимо установить утилиту awscli для доступа к файлам, которые расположены в s3. 
```apt install -y awscli```

Скачиваем данные Нью - Йоркского такси за 2020г. 
```aws s3 cp s3://nyc-tlc/trip\ data/yellow_tripdata_2020-12.csv ./ --no-sign-request```

Загрузим скачаный файл на HDFS с размером блока 64 Мб (в коде в байтах) и двойной репликацией. 
```hadoop fs -Ddfs.blocksize=67108864 -Ddfs.replication=2 -put yellow_tripdata_2020-12.csv```

Проверим информацию по блокам
```hdfs fsck yellow_tripdata_2020-12.csv -blocks -locations```

Интеграция Hadoop и s3 
```hadoop fs -Dfs.s3a.endpoint=s3.amazonaws.com -Dfs.s3a.aws.credentials.provider-org.apache.hadoop.fs.s3a.AnonymousAWSCredentialProvider -ls s3a://nyc-tls/trip\ data/yellow_tripdata_2020-11.csv```

Копируем данные с s3 на HDFS
```hadoop fs -mkdir 2019 hadoop distcp -Dfs.s5a.endpoint=s3.amazonaws.com -Dfs.s5a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider s3a://nyc-tic/trip\ data/yellow_tripdata_2019-1* 2019/```

Посмотрим что скачалось, первые 10 строк.
```hadoop fs -text 2019/yellow_tripdata_2019-10.csv | head -n 10```

