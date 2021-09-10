# Spark
Criar login:
https://community.cloud.databricks.com/

Spark
projeto de processamento de addos, iniciado em 2009. originalmente de codigo aberto. projeto com mais de 400 contribuidores e committers de grandes empresas. Escrito em Scala, em cima de JVM e Java runtime. Executa no Windows, Linux, Apple Mac. utilizado por mais de 500 organizações.

Como abstração: permite que desenvolvedores criem rotinas de processamento de dados complexos e de varios estágios, fornecendo uma api de alto nível e uma estrutura tolerante a falhas que permite aos programadores se concentrarem na logica em vez de questoes ambientais ou de infra , como falha de hardware

É rapido, eficiente e escalonável. Implementa uma estrutura distribuida e tolerante a falhas na memória chamada RDD. O Spark maximiza o uso de memoria em varias maquinas melhorando o desempenho geral em ordens de magnitude. A reutilização dessas estruturas na memoria pelo Spark o torna adequado para operações iterativas de aprendizado de maquina, bem como consultas interativas.


Aplicações:

- Operações ETL (map(transformação) e filter)
- Análise preditiva e aprendizado de máquina
- Operações de acesso a dados (como consultas e visualizações SQL)
- Mineração e processamento de texto (analise de sentimento pelo GraphX)
- Processamento de eventos em tempo real
- Aplicativos gráficos
- Reconhecimento de padrões
- Mecanismos de recomendação

Programação
Scala; Python; Java; SQL; R

Exemplos RDD:

Python
rddArquivo = sc.textFile ("hdfs://meucluster/data/arquivo.txt")

Scala
val rddArquivo = sc.textFile("hdfs:// meucluster/data/arquivo.txt")

Java
JavaRDD<String> rddArquivo = sc.textFile("hdfs://meucluster/data/arquivo.txt");

Terminal:

#Interativo:
#acessa hdfs, cloud, preparar o codigo. nao é usado para produção. 

Uso interativo: PySpark
Uso interativo: Scala:

Uso não-interativo (para produção):

#spark-submit:
$ spark-submit \
--master yarn \
--queue "SquadFI" \ #fila, boa pratica
--name "[Programa Spark 123] ETL 999" \
--driver-memory 2G \
--executor-memory 2G \
--executor-cores 1 \
--proxy-user hive \
--conf "spark.driver.maxResultSize=16g" \
--conf "spark.dynamicAllocation.enabled=true" \
--conf "spark.shuffle.service.enabled=true" \
--conf "spark.shuffle.service.port=7337" \
--conf "spark.dynamicAllocation.initialExecutors=10" \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.maxExecutors=80" \
--conf "spark.yarn.driver.memoryOverhead=2000" \
--conf "spark.yarn.executor.memoryOverhead=2000" \
--driver-java-options "-Djavax.security.auth.useSubjectCredsOnly=false" \
--jars commons-csv-1.2.jar,spark-csv_2.11-1.5.0.jar \
Main.py <parametro 1> <parametro 2> <parametro N>

Anatomia

1. Driver
2. Cluster Manager
3. Spark Master
4. Executors

Anatomia

DRIVER
A vida útil do aplicativo Spark começa (e termina) com o driver Spark. é o processo que os clientes usam para enviar aplicativos no Spark. Responsável por planejar e coordenar a execução do programa Spark e retornar o status e/ou resultados (dados) ao cliente. cria o SparkContext. Planeja o aplicativo. Obtém a entrada de processamento do aplicativo e planeja a execução do programa. Obtem todas as transformações solicitadas (operações de manipulação de dados) e ações (solicitações de saída ou um prompt para executar o programa) e cria um Gráfico Acíclico Direcionado (Directed Acyclic Graph - DAG).

O SparkContext, é referido como sc, é a instância do aplicativo que representa a conexão com o Master do Spark (e os Workers do Spark). O SparkContext é instanciado no início de um aplicativo Spark (incluindo os shells interativos) e é usado para todo o programa.

Assim como acontece com os pontos de entrada do programa SparkContext, SQLContext e HiveContext, os aplicativos Spark Streaming também têm um ponto de entrada chamado StreamingContext

Aplicação Spark = Driver + grupo de Executors

DAG
#Abrir o dag em caso de problemas
Anatomia
Stage0>TextFile>flatMap>map>
Stage1>reduceByKey>ShuffleRDD[4]

Obs.: Parquet não aceita evolução de schema. neste caso tem q fazer refactor, criar tabela temporaria, p cada tabela, q recria tabelas. cria coluna no skala e insere. 

Driver executa o processo principal e cria um SparkContext que serve para coordenar a execução do seu job.
Os executors são processos em execução nos work nodes responsável por executar tasks que o Driver atribuiu a ele.

O SparkContext é usado pelo processo do Spark Driver do seu aplicativo Spark para estabelecer uma comunicação com o cluster e o cluster manager (Yarn) para então coordenar e executar jobs.

SparkContext também permite o acesso a outros dois contextos, ou seja, SQLContext e HiveContext.

Para criar um SparkContext, primeiro você precisa criar uma configuração, chamada de SparkConf.

SQLContext é o ponto de entrada para SparkSQL, que é um módulo Spark para processamento de dados estruturados.

Depois que o SQLContext é inicializado, o usuário pode usá-lo para realizar várias operações "semelhantes a sql" em conjuntos de dados e dataframes.

Para criar um SQLContext, primeiro você precisa instanciar um SparkContext.

Se o seu aplicativo Spark precisa se comunicar com o Hive e você está usando o Spark <2.0, provavelmente precisará de um HiveContext.



CLUSTER MANAGER
É o processo responsável por monitorar os nós Workers e reservar recursos nesses nós mediante solicitação do Spark Master. O Spark Master, por sua vez, disponibiliza esses recursos de cluster para o Driver na forma de executores. Pode ser separado do processo Master. Esse é o caso ao executar o Spark no Mesos ou YARN.

No caso do Spark rodando no modo Standalone, o processo Master também executa as funções do Cluster Manager. Efetivamente, ele atua como seu próprio gerenciador de cluster.

O Cluster Manager em um aplicativo Spark distribuído é o processo que governa, monitora e reserva recursos na forma de contêineres em nós de worker de cluster (ou escravos). Esses contêineres são reservados mediante solicitação do Spark Master. O Cluster Manager no caso do Spark no YARN é o YARN ResourceManager.

Um Spark Driver em execução no modo YARN envia um aplicativo ao ResourceManager e, em seguida, o ResourceManager designa um ApplicationsMaster para o aplicativo Spark.

O cluster manager (Yarn, Mesos) é responsável pela alocação de recursos para seu aplicativo Spark.

Anatomia

CLUSTER MANAGER
Exemplo de um aplicativo Spark gerenciado pelo YARN mostrado no ResourceManager UI, normalmente disponível em http://<resource_manager>:8088

SPARK MASTER
O Spark master é o processo que solicita recursos no cluster e os disponibiliza para o driver Spark. Em qualquer modo de implantação, o mestre negocia recursos ou contêineres com nós de trabalho ou nós escravos e rastreia seu status e monitora seu
progresso.

O processo mestre do Spark atende a uma interface de usuário da web na porta 8080 no host mestre:

SPARK MASTER
O processo ApplicationsMaster que é instanciado pelo ResourceManager no envio do aplicativo Spark atua como oSpark Master. O Driver informa o ApplicationsMaster sobre os requisitos de seu executor para o aplicativo. O ApplicationsMaster, por sua vez, solicita containers (que são hospedados em NodeManagers) do ResourceManager para hospedar esses executores.
O ApplicationsMaster é responsável por gerenciar esses containers (executors) durante o ciclo de vida do aplicativo
Spark.
O Driver coordena o estado do aplicativo e as transições de estágio de processamento.
 O próprio ApplicationsMaster é hospedado em uma JVM em um nó slave ou worker no cluster e é, na verdade, o primeiro recurso alocado para qualquer aplicativo Spark em execução no YARN.

SPARK MASTER
A UI do ApplicationsMaster para um aplicativo Spark é a UI do Spark Master. Isso está disponível como um link na UI do ResourceManager.

Modo Cluster
Client>YARN Resource Manager>NodeManager(Applications Master)

Modo Cluster (Yarn-Cluster)
1.O Client (uma chamada de processo do usuário via spark-submit) envia um aplicativo Spark para o Cluster Manager (o YARN ResourceManager).
2.O ResourceManager atribui um ApplicationsMaster (o Spark Master) para o aplicativo. O processo do Driver é criado no mesmo nó.
3.O ApplicationsMaster solicita containers para os Executors do ResourceManager. Os contêineres são atribuídos e os executors são gerados. O Driver então se comunica com os executors para organizar o processamento de tarefas e estágios do programa Spark.
4.O driver retorna o progresso, resultados e status para o cliente.

o código do seu aplicativo ...



Cloud 

AWS:

acesso via ssh:

EMR (Managed Hadoop Framework)

Elastic MapReduce



AWS Glue DataBrew (Visual data preparation tool to clean and normalize data for analytics and machine learning)
Mais acessivel que o EMR, so liga o job qdo necessário

Cloud para testes:
Databricks Community (https://community.cloud.databricks.com/)


Spark Streaming

Por que?
Processamento de eventos em tempo real em sistemas de big data
De sensores e processamento de dados de rede à detecção de fraudes e monitoramento de sites e muito mais
Capacidade de consumir, processar e obter insights de fontes de dados de streaming

Os objetivos para Spark Streaming incluem:
Latência baixa (segunda escala)
Processamento de evento único (apenas uma vez)
Escalabilidade linear
Integração com Spark Core API

Necessidade do processamento de eventos/fluxos como um componente-chave
Processamento de eventos integrado com sua estrutura de lote baseada em RDD
A abordagem Spark Streaming
Spark Streaming apresenta o conceito de "discretized streams" (ou DStreams)
DStreams são essencialmente lotes de dados armazenados em vários RDDs, cada lote representando uma janela de tempo (normalmente em segundos)
Os RDDs resultantes podem ser processados usando a API Spark RDD principal e todas as transformações disponíveis

Macro arquitetura
Data Stream>park Streaming>Dstream (Time windowed RDDs)>Spark>>Processed Results

Avro, arquivo parecido com json, chave valor, 
RDD é uma sequencia de linhas
Spark processa os lotes do RDD






--------------------------


O Spark 2.0 introduziu o SparkSession que substituiu essencialmente SQLContext e HiveContext e concede acesso imediato ao SparkContext.


---------------------------

StreamingContext

O StreamingContext representa uma conexão a uma plataforma ou cluster Spark usando um SparkContext existente
O StreamingContext é usado para criar os datasources de DStreams controlar a computação de streaming e as transformações de DStream.
O StreamingContext também especifica o argumento batchDuration, que é um intervalo de tempo em segundos pelo qual os dados de streaming serão divididos em lotes.
Depois de instanciar um StreamingContext, você criaria uma conexão com um fluxo de dados e definiria uma série de transformações a serem realizadas.
O método start() ou ssc.start() é usado para acionar a os dados de entrada depois que um
StreamingContext é estabelecido.
O StreamingContext pode ser interrompido programaticamente usando ssc.stop()ou ssc.awaitTermination().

Discretized streams (DStreams) são o objeto de programação básico na API Spark Streaming
Streams representam uma sequência contínua de RDDs que são criados a partir de um fluxo contínuo de dados
DStreams podem ser criados a partir de fontes de dados de streaming, como soquetes TCP, sistemas de mensagens, APIs de streaming (como a API de streaming do Twitter) e muito mais.
DStreams (como uma abstracção RDD) também pode ser produzido a partir de transformações realizadas na DStreams existentes (tais como map, flatMap e outras operações).
DStreams oferece suporte a dois tipos de operações:
-Transformações
-Operações de saída

DStreams são Lazy Evaluation assim como Spark RDD
Representação de um discretized stream, com cada intervalo t representando uma janela de tempo especificada pelo batchDurationargumento na instanciação de StreamingContext:

Data Stream>>
Intervalo=0 >> Dado0\nDado1
Intervalo=1 >> Dado2\nDado3
Intervalo=n >>Dadon\nDadon
>
>Discretized Stream (DStream)>>
>Dado0 Dado1 >>>> RDD Intervalo=1
>Dado2 Dado3 >>>> RDD Intervalo=2
>Dadon\nDadon >>>> RDD Intervalo=N

DStreams Source são definidos em um StreamingContext para um fluxo de dados de entrada especificado, da mesma forma que os RDDs são criados para uma fonte de dados de entrada em um SparkContext. 
Muitas fontes de entrada de streaming comuns estão incluídas na API de streaming, como fontes para ler dados de um soquete TCP ou para ler dados enquanto eles estão sendo gravados no HDFS
As fontes básicas de dados de entrada para a criação de DStreams são descritas aqui:
socketTextStream()
StreamingContext.socketTextStream (hostname, port, storageLevel = StorageLevel (True, True, False, False, 2))
O método socketTextStream é usado para criar um DStream a partir de uma fonte TCP de entrada definida pelos argumentos hostnamee e port.
Os dados recebidos são interpretados usando a enconding UTF8, com terminação de nova linha usada para definir novos registros.
O storageLevelargumento que define o storage level padrão MEMORY_AND_DISK_SER_2



Caso Telecom
Dashboard, qtas pessoas de cada Estado, identifica qtas mensagens/sms(mcvm) enviadas (msvs)

Celular>>Microservice/API >> Spark Streaming>>HBase>>Apache Phoenix>>MicroStrategy(Data Vis)



Compatibilidade:

Kafka
Flume (Em desuso)
HDFS/S3
Kineses
Twiter
>
>Spark Streaming
>
>HDFS
>Databases
>Dashboards

akka
kafka
hbase
mysql
mongoDB
Parquet
PostgreSQL
static data sources
elastic search
cassandra
>>>>>>>>>>>>>>>>>>>
Spark Streaming <<<<<<>>>>>MLlib
Spark Streaming <<<<<<>>>>>SparkSQL
>
>Data Storage Systems
>
>memsql
>elasticsearch
>cassandra
>hbase
>kafka
>parquet

Dados
Caso de uso:

Usuário/cliente>>criar painel movimentação de vendas por produto>>produto>>preço>>qde vendida p produto>>impostos/taxas>> para comparar um periodo de 10 anos, visao diária, mensal, anual>>balanço do dia fechado as 7 do dia posterior.>>>
p/ comparar dia deste ano com o mesmo do ano passado>>>

Que tipo de informação podemos extrair?

Lógico:
1. Procurar/mapear informações de produtos em alguma base de dados de produto (mainframe, db sql, cloud, api)
2. Criar db histórica/vegetativa de até 10 anos
3. Possível ingestão de dados Batch (dia -1 = conta dados de ontem até -10 anos). 
Boa prática: Criar rotina de expurgo. Não depositar tudo no Data Lake.

Físico:
Qual ferramenta utilizar? 
Boa prática: não compactar em tar.gz no hdfs.
1. Qual a melhor estratégia de particionamento? Boa prática, para arquivos pequenos, consumiria um bloco inteiro de hdfs, deve fazer o join de arquivos, repartition, workflow novo para reprocessamento, outra tabela temporaria, le tudo e junta td na mesma partição. 
2. Qual banco de dados devo utilizar? Armazenamento, onde deixar a informação, como será a consulta? Tableu?
3. Qual tipo de arquivo de arquivo devo utilizar? Dependendo do db. txt, scv, parquet, json, avro, zip, etc.
4. Qual tipo de compressão devo utilizar?

Arquitetura Data Lake

data injestion>>file system>>armazenamento>>interface>>data visualization>>enriquecimento>>rest

On Premises
Data Center/Data Lake/No cliente
Problemas: Governança, se não houver boa governança não há controle. Não há padrão, tipos, etc. Não há controle de espaço de armazenamento.
Várias fontes de dados

Obs.: caso um arquivo de 10mb ocupe um bloco, para recuperar essse dado, pode demorar mais de hora para visualizar no tableau /power bi, microStrategy, etc.

----------

Cloud (AWS)

Mais de 200 serviços. 

Glue (User friendly)
emr (cluster - spark puro)

ETL em glue ou emr>>armazenado em DynamoDB/Bucket S3/MySQL

Guia de custo e configuração de máquinas: https://aws.amazon.com/pt/ec2/pricing/on-demand/

DynamoDB: https://aws.amazon.com/pt/dynamodb/pricing/on-demand/

CloudWatch (Logs): https://aws.amazon.com/pt/cloudwatch/pricing/

---------------

Camadas File System
Pastas dentro do file system

Raw Zone
Comum, onde garda info da origem, bruta. Pode armazenar em Parquet, o arquivo original é deletado.

Trusted Zone
Onde faz  o etl, faz um aquivo consolidado, um join.

Refined Zone
Onde especifica os dados necessários , um select, e filtra os dados.

Em Big Data existem vários formatos de dados. Em um formato legível como arquivo JSON ou CSV, mas isso não significa que essa é a melhor maneira de realmente armazenar os dados.
Existem três formatos de arquivo otimizados para uso em clusters Hadoop:

-Optimized Row Columnar (ORC)
-Avro
-Parquet

Tipos de armaz.
Avro (ideal de evoluçao de schema/colunas)
Parquet (não ideal caso precise mudar o schema). Compressão muito boa, melhor que o avro
Orc (compressão excelente, estabilidade)

Parquet

- -Orientado por coluna (armazenar dados em colunas): os armazenamentos de dados orientados por coluna são otimizados para cargas de trabalho analíticas pesadas em leitura
- -Altas taxas de compressão (até 75% com compressão Snappy)
- -Apenas as colunas necessárias seriam buscadas / lidas (reduzindo a E / S do disco)
- -Pode ser lido e escrito usando Avro API e Avro Schema

Avro
- Com base em linha (armazenar dados em linhas): bancos de dados baseados em linha são melhores para cargas de
trabalho transacionais pesadas de gravação
- Serialização de suporte
- Formato binário rápido
- Suporta compressão de bloco e divisível
- Evolução do esquema de suporte (o uso de JSON para descrever os dados, enquanto usa o formato binário para
otimizar o tamanho do armazenamento)
- Armazena o esquema no cabeçalho do arquivo para que os dados sejam autodescritivos
Referência: https://avro.apache.org/docs/1.10.1/

ORC

- -Orientado por coluna (armazenar dados em colunas): os armazenamentos de dados orientados por coluna são otimizados para cargas de trabalho analíticas pesadas em leitura
- -Altas taxas de compressão (ZLIB)
  Suporte ao tipo Hive (datetime, decimal e os tipos complexos como struct, list, map e union)
- -Metadados armazenados usando buffers de protocolo, que permitem adição e remoção de campos
- -Compatível com HiveQL
- -Suporte a Serialização

Camadas
Cada uma das 3 camadas se tratam de subdiretórios dentro do sistema de arquivos distribuídos, HDFS, e podem ser mapeados através da propriedade LOCATION.

HDFS

https://www.kaggle.com/gpreda/covid-world-vaccination-progress
/FileStore/tables/country_vaccinations.csv



Referências

Criando pipelines de dados eficientes com Spark e Python
https://web.digitalinnovation.one/course/criando-pipelines-de-dados-eficientes-com-spark-e-python



Documentação: https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html

Safari Books: 
https://www.safaribooksonline.com/


DB105 - Spark Programming (reference guide)
Certificações Cloudera: 
https://www.cloudera.com/about/training/certification/cca-spark.html
https://www.cloudera.com/about/training/certification/ccp-data-engineer.html

Certificações Cloudera (CCA 175): https://www.cloudera.com/about/training/certification/cca-spark.html

Certificações Cloudera (DE575): https://www.cloudera.com/about/training/certification/ccp-data-engineer.html

Certificações Databricks: 
https://academy.databricks.com/category/certifications
https://academy.databricks.com/exam/INT-ADAS-v2-CT





​	
