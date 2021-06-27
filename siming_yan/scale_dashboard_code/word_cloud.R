library(wordcloud)
# install.packages("RColorBrewer")
library(RColorBrewer)
# install.packages("wordcloud2")
library(wordcloud2)
library(tidyverse)
# getwd()
levin = data.frame(readxl::read_excel("./BMS_ScaleWMOS20210427.xlsx", sheet = 1))
# ---------
names(levin)[6] =  "names"
levin$names = gsub("费用|费|收费","",levin$names)
levin2 = data.frame(levin$names)
# write.csv(levin2, "./BMS_to_paddle.csv")

levin2$generic_naems = 0
View(levin2 %>% group_by(levin.names) %>% summarise(n()))
# ---------
levin2[str_detect(levin2$levin.names, "报关"),"generic_naems"] = "报关"
levin2[str_detect(levin2$levin.names, "出库"),"generic_naems"] = "出库"
levin2[str_detect(levin2$levin.names, "入库"),"generic_naems"] = "入库"
levin2[str_detect(levin2$levin.names, "清点"),"generic_naems"] = "清点"
levin2[str_detect(levin2$levin.names, "加班"),"generic_naems"] = "加班"
levin2[str_detect(levin2$levin.names, "仓储"),"generic_naems"] = "仓储"
levin2[str_detect(levin2$levin.names, "运输"),"generic_naems"] = "运输"
# levin2[str_detect(levin2$levin.names, "操作"),"generic_naems"] = "操作"
levin2[str_detect(levin2$levin.names, "工资"),"generic_naems"] = "工资"
levin2[str_detect(levin2$levin.names, "叉车"),"generic_naems"] = "叉车"
levin2[str_detect(levin2$levin.names, "管理"),"generic_naems"] = "管理"
levin2[str_detect(levin2$levin.names, "仓租"),"generic_naems"] = "仓租"
levin2[str_detect(levin2$levin.names, "水电"),"generic_naems"] = "水电"
levin2[str_detect(levin2$levin.names, "电"  ),"generic_naems"] = "水电"
levin2[str_detect(levin2$levin.names, "能源"),"generic_naems"] = "水电"

levin2[levin2$generic_naems == 0, "generic_naems"] = 
  levin2[levin2$generic_naems == 0, "levin.names"]
# ---------
levin3 = merge((levin2 %>% group_by(generic_naems) %>% summarise(n())), 
              levin2, 
              by = "generic_naems")
levin3 =levin3[,1:2] %>% distinct()
levin3 = levin3[levin3$`n()`>1, ]
# wordcloud(words = levin$names,
#           freq = levin$`n()`,
#           min.freq = 0,
#           random.order=FALSE,
#           rot.per=0.35,
#           colors=brewer.pal(8, "Dark2"))


# ------------
hw = wordcloud2(data = levin3, 
                shape = "diamond", 
           size = 2,
           minRotation = 35, maxRotation = 100)
hw
# install.packages("webshot")
# webshot::install_phantomjs()
library(htmlwidgets)
# library(wordcloud2)
# hw <- wordcloud2(demoFreq,size = 3)
saveWidget(hw,"2.html",selfcontained = F)
webshot::webshot("2.html","2.png",vwidth = 1992, vheight = 1744, delay =10)

# ------------
hw2 = wordcloud2(data = levin2, shape = "circle", 
                size = 3,
                minRotation = 35, maxRotation = 100)
hw2
saveWidget(hw,"2.html",selfcontained = F)
webshot::webshot("2.html","2.png",vwidth = 1992, vheight = 1744, delay =10)


# write.csv(levin3,"./word_cloud_bms.csv",  row.names = F)
