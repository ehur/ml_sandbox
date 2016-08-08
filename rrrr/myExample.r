# Init
libs <- c("tm", "plyr", "class")
lapply(libs,require, character.only= TRUE)
# Set Options
options(stringsAsFactors = FALSE)

incentives <- read.csv("~/git/ml_sandbox/rrrr/incentives.csv", header = TRUE)
shopping <- read.csv("~/git/ml_sandbox/rrrr/shopping.csv", header = TRUE)

cleanCorpus <- function(corpus){
  corpus.tmp <- tm_map(corpus, removePunctuation)
  corpus.tmp <- tm_map(corpus.tmp,stripWhitespace)
  corpus.tmp <- tm_map(corpus.tmp,tolower)
  corpus.tmp <- tm_map(corpus.tmp,removeNumbers)
  corpus.tmp <- tm_map(corpus.tmp, PlainTextDocument)
  corpus.tmp <- tm_map(corpus.tmp,removeWords, stopwords("english"))
  return(corpus.tmp)
}

dfIncentives <- data.frame(incentives)
dfShopping <- d


generateTDM <- function(classification,df){	##-- DataframeSource worked too, but seems to add complexity. Favored the VectorSource
  s.cor <- Corpus(DataframeSource(df[-2]))
  s.cor.cl <- cleanCorpus(s.cor)
  s.tdm <- TermDocumentMatrix(s.cor.cl)
  s.tdm <- removeSparseTerms(s.tdm, 0.9)
  result <- list(name = classification, tdm = s.tdm)
}

generateTDMV <- function(classification,data){
  s.cor <- Corpus(VectorSource(data)) 
  s.cor.cl <- cleanCorpus(s.cor)
  s.tdm <- TermDocumentMatrix(s.cor.cl)
  s.tdm <- removeSparseTerms(s.tdm, 0.9) ##-- sparsity of 0.7 removed too many terms - and we only ended up with a handful! thie leaves us with 97 and 53 respectively
  result <- list(name = classification, tdm = s.tdm)
}

tdm1 <- generateTDM("incentives",incentives)
tdm2 <- generateTDM("shopping",shopping)

tdmV1 <- generateTDMV("incentives",incentives[,2])
tdmV2 <- generateTDMV("shopping",shopping[,2])

tdm <- list(tdm1,tdm2)
  

tdmX <- lapply(candidates,generateTDMV)

# Attach Classification
bindClassificationToTDM <- function(tdm) {
  s.mat <- t(data.matrix(tdm[["tdm"]]))
  s.df <- as.data.frame(s.mat, stringAsFactors = FALSE)
  s.df <-cbind (s.df, rep(tdm[["name"]], nrow(s.df)))
  colnames(s.df)[ncol(s.df)] <- "classification"
  return(s.df)
  }

classTDM <- lapply(tdm,bindClassificationToTDM)

//*** to here 3/23/16:
write texts to individual files. Use this in a function:
output <- "/Users/lhurley/git/ml_sandbox/rrrr/output.txt"
fileConn<-file("/Users/lhurley/git/ml_sandbox/rrrr/output.txt")
writeLines(c("Hello","World"), fileConn)
close(fileConn)

indices<-seq(1:nrow(shopping))
writeFile <- function(dirName,fileName,fileContent){
  pwd<-"/Users/lhurley/git/ml_sandbox/rrrr"
  pathToWrite<-paste(pwd,"/",dirname,"/",fileName,".csv",sep ="")
  fileConn<-file(pathToWrite)
  writeLines(fileContent, fileConn)
  close(fileConn)
}

and call it with lapply over shopping[2] (the second col in shopping data set)

