val myNums = Array(12250,5217.2,90124,20052.345,5335,6051.052,3266.789,2231.7,1083.992,6608.424,5347,2138.979,78260.265,7700,3425.647,7466.434,19494.412,366196.5,2874.415,13422.521,5381.471,8165.832,17410,22744.572)
val n = myNums.size
val mySum = myNums.sum
val myMean = myNums.sum / n
val sumDevs = myNums.map(element => (element - myMean) * (element - myMean)).sum
val stdDevs = Math.sqrt(sumDevs/(n-1))

val normalized = myNums.map(element => (element - myMean) / stdDevs )



val docs = Array("first","word","again","word","uf","something","word","WORD")

val result = docs.foldLeft(0)((r:Int,c:String) => if (c.toLowerCase() == "word") r + 1 else r)
val line="12345,\"this is the description, I would say\""
val splitLine = line.split(",",2)
val words = """\b\p{L}+\b""".r
val sentence = "this ~is a... you know - a sentence, \with some stuff! in it. Actually it's two..."
words findAllIn sentence foreach println
