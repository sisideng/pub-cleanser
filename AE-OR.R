library(naniar)
library(tidyverse)
library(lubridate)
library(xlsx)

# import ignition audit data
raw_ign_audit <- read_csv("C:/Users/40205009/Documents/R_Project/PUB/audit.csv")
# import Exaquantum Alarm tables
raw_eq_or <- read_tsv("C:/Users/40205009/Documents/R_Project/PUB/OR.txt")


hr <- c(8:24)
dummy<- raw_eq_or$TimeStamp[!str_detect(raw_eq_or$TimeStamp,"^2019")]
dummy1 <- dummy[1]              # Title line in each individual source file
dummy2 <- dummy[length(dummy)]  # NA line 

ign_audit <- raw_ign_audit %>% 
  #filter(!is.na(EVENT_TIMESTAMP)& EVENT_TIMESTAMP != "EVENT_TIMESTAMP") %>%
  mutate (EVENT_TIMESTAMP=as.POSIXct(EVENT_TIMESTAMP,format= "%d/%m/%Y %I:%M:%S %p")) %>%
  mutate(update=paste(EVENT_TIMESTAMP, ACTOR, ACTION,ACTOR_HOST,ACTION_TARGET,ACTION_VALUE)) %>%
  select(update, everything()) %>%
  filter(hour(EVENT_TIMESTAMP) %in% hr) %>%
  mutate(date=day(EVENT_TIMESTAMP)) %>%
  mutate(hour=hour(EVENT_TIMESTAMP))

or <- raw_eq_or %>% 
  #filter(TimeStamp != dummy1 & TimeStamp != dummy2) %>% 
  select(-TimeStampNS, -OPCServerID, -Cookie, -Severity) %>% 
  mutate(TimeStamp=format(as.POSIXct(as.character(TimeStamp),tz="UTC"),tz="Singapore")) %>%
  filter(hour(TimeStamp) %in% hr) %>%
#  filter(minute(TimeStamp)<20 |(minute(TimeStamp)==20 & second(TimeStamp)==0)) %>%
  mutate(update=paste(TimeStamp, ActorID, IGN2_Action_Type, IGN2_Actor_Host,IGN2_Action_Target, IGN2_Action_Value)) %>%
  select(update, everything()) %>%
  mutate(date=day(TimeStamp)) %>%
  mutate(hour=hour(TimeStamp))


ign_audit$EVENT_TIMESTAMP <- as.character(ign_audit$EVENT_TIMESTAMP)
or$TimeStamp <- as.character(or$TimeStamp)
ign_audit <- ign_audit %>% rename("update (= TIMESTAMP+ACTOR+ACTION+ACTOR_HOST+TARGET+VALUE)" = update)
or <- or %>% rename("update (= TimeStamp+ActorID+Action_Type+Host+Target+Value)" = update)
write_csv(ign_audit,"SCADA_Audit.csv")
write_csv(or,"EQ_Ope_Record.csv")

outputfilename <- "WOWW_Event_comparison.xlsx"
write.xlsx(as.data.frame(ign_audit),file=outputfilename,sheetName="SCADA_Audit",row.names=F,showNA=F)
write.xlsx(as.data.frame(or), file=outputfilename,sheetName="EQ_Op_Record",append=TRUE,row.names=F,showNA=F)

merged_or <- merge(ign_audit,or, by="update", all=T)
write_csv(merged_or,"merged_or.csv")

#write_csv(or,"ca_or_trimed.csv")
#write_csv(ign_audit,"ca_ign_audit_trimed.csv")