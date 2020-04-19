library(naniar)
library(tidyverse)
library(lubridate)
library(xlsx)

# import Ignition Alarm (.txt)
raw_ign_alm <- read_tsv("C:/Users/40205009/Documents/R_Project/PUB/alarm.txt")
# import Exaquantum Alarm Table 
raw_eq_pa <- read_tsv("C:/Users/40205009/Documents/R_Project/PUB/pa.txt")

# eventtype 0= (alarm), 1= RTN, 2=ACKED
# pa-Severity -> Ign-Priority 300 -> 1; 500 -> 2; 900 -> 3;1000 -> 4
hr <- c(10,14,22)

ign_alm <-  raw_ign_alm %>% select(-eventid) %>% mutate(new=source) %>%
  extract(new,c("source_Scr","source_MGS"),regex="prov:.*:/tag:(.*):/alm:(.*)") %>%
  # mutate (eventtime=as.POSIXct(str_replace(eventtime,"\\.\\d{3}$",""),format= "%Y-%m-%d %H:%M:%S")) %>%
  mutate (eventtime=as.POSIXct(eventtime,format= "%d/%m/%Y %I:%M:%S %p")) %>%
  filter(hour(eventtime) %in% hr) %>%
  mutate(source_MGS = tolower(source_MGS)) %>%
  mutate_all(str_trim) %>% 
  mutate(update=paste(source_Scr,source_MGS,"EventType-",eventtype,eventtime, "Flag-",eventflags)) %>%
  select(update, everything()) %>% 
  mutate(day=day(eventtime)) %>%
  mutate(hour=hour(eventtime))


pa <- raw_eq_pa %>% filter(Source != "Source") %>% 
  select(- TimeStampNS, -OPCServerID, -Cookie, -ActorID, -ChangeMask, -NewState, -ConditionQuality, -AckRequired,-AckComment,-ActiveTime) %>% 
  separate(Message,c("Message_MGS","Message_EVNTTP"),sep="\\s\\(\\s", fill="right",extra="merge")%>% 
  mutate(Message_EVNTTP = str_replace(Message_EVNTTP,"RTN \\)","1")) %>%
  mutate(Message_EVNTTP = str_replace(Message_EVNTTP,"ACKED \\)","2")) %>% 
  mutate(Message_EVNTTP = replace_na(Message_EVNTTP,"0")) %>%
  mutate(Message_MGS = tolower(str_replace(Message_MGS,"^Alm:",""))) %>%
  mutate(Severity_Pr=Severity) %>%
  mutate(Severity_Pr= str_replace(Severity_Pr,"300","1" )) %>%
  mutate(Severity_Pr= str_replace(Severity_Pr,"500","2" )) %>%
  mutate(Severity_Pr= str_replace(Severity_Pr,"900","3" )) %>%
  mutate(Severity_Pr= str_replace(Severity_Pr,"1000","4" )) %>%
  mutate(TimeStamp=format(as.POSIXct(as.character(TimeStamp),tz="UTC"),tz="Singapore")) %>%
  filter(hour(TimeStamp) %in% hr) %>%
  #filter(minute(TimeStamp)<20 |(minute(TimeStamp)==20 & second(TimeStamp)==0)) %>%
  mutate_all(str_trim) %>% 
  mutate(update=paste(Source,Message_MGS,"EventType-",Message_EVNTTP,TimeStamp,"Flag-",IGN3_Event_Flags)) %>%
  select(update, everything()) %>%
  mutate(day=day(TimeStamp)) %>%
  mutate(hour=hour(TimeStamp))
 



################ print tables#######################
ign_alm <- ign_alm %>% rename("update (=source_Scr+source_MGS +eventtype +eventtime +Flag"=update)
pa <- pa %>% rename("update (= Source+Message_MGS +EventType+TimeStamp+Flag)" = update)
pa$TimeStamp <- as.character(pa$TimeStamp)
ign_alm$eventtime <- as.character(ign_alm$eventtime)

outputfilename <- "WOWW_Alarm_comparison.xlsx"
write.xlsx(as.data.frame(ign_alm),file=outputfilename, sheetName="SCADA_Alarms",row.names=F,showNA=F)
write.xlsx(as.data.frame(pa), file=outputfilename, sheetName="EQ_Process_Alm",append=TRUE,row.names=F,showNA=F)

############## save to csv ########
write_csv(ign_alm,"SCADA_Alarms.csv")
write_csv(pa,"EQ_Process_Alm.csv")


################ merge tables#######################
ign_alm <- ign_alm %>% select(-hour,-day)
pa <- pa %>% select(-hour,-day) %>% rename("update"= "update (= Source+Message_MGS +EventType+TimeStamp+Flag)" )
merged <- merge(ign_alm,pa, by="update", all=T)
merged$TimeStamp<- as.character(merged$TimeStamp)
merged$eventtime <- as.character(merged$eventtime)
write_csv(merged,"PA_outerjoin.csv")
             