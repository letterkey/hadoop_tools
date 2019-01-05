運行方法:
1.進入項目目錄
2.運行 mvn assembly:assembly 
3.從target目錄中找到編譯後的jar文件
4.上傳jar到hadoop
5.運行命令:hadoop jar hdfsToInfoBright-0.0.1-SNAPSHOT-jar-with-dependencies.jar  /tmp/scala.txt /out db-server:3306/ymy_test zhaohaijun 676892 scala 1000
hadoop命令參數描述:/tmp/scala.txt:數據源輸入目錄
 /out:hadoop job的輸入目錄