start-all.sh
stop-all.sh

Definir variáveis

    export HDFS_NAMENODE_USER="root"
    export HDFS_DATANODE_USER="root"
    export HDFS_SECONDARYNAMENODE_USER="root"
    export YARN_RESOURCEMANAGER_USER="root"
    export YARN_NODEMANAGER_USER="root"

Compilar

    $javac WordCount.java -cp $(hadoop classpath)
    $jar cf wc.jar WordCount*.class

    $javac Kmeans.java -cp $(hadoop classpath)
    $jar cf kmeans.jar Kmeans*.class

    $javac Doc2Point.java

Mover arquivos para o hdfs

    $hadoop fs -put /home/douglas/Dropbox/workspace/md/input /user/root
    $hadoop fs -put /home/douglas/Dropbox/workspace/md/word_count_per_doc /user/root
    $hadoop fs -put /home/douglas/Dropbox/workspace/md/centroids /user/root

Mover arquivo para o local

    $hadoop fs -get /user/root/output-wc/part-r-00000 /home/douglas/Dropbox/workspace/md

Apagar

    $hadoop fs -rm nomeArquivo
    $hadoop fs -rm -r nomeDiretorio

Executar

    $hadoop jar wc.jar WordCount input output-wc
    $hadoop jar knn.jar KNN word_count_per_doc centroids
    $java Doc2Point part-r-00000

Exibir output

    $hadoop fs -cat output-wc/part-r-00000






