# clean the slate
rm(list=ls()); gc()

setwd("C:/repo/GROUSE")

pacman::p_load(
  dplyr
  ,tidyr
  ,magrittr
  ,stringr
  ,ggplot2
  ,maps
  ,gridExtra
  ,grid
  ,ggthemes
  ,scales
  ,ggrepel
  ,ggpubr
  # ,zipcode
)

dt1<-read.csv("./data/xwalk_summary_orig.csv",stringsAsFactors = F)
dt2<-read.csv("./data/xwalk_summary_demo.csv",stringsAsFactors = F)
dt3<-read.csv("./data/xwalk_summary_3cohorts.csv",stringsAsFactors = F)

# overview
dt3_enc<-dt3 %>% 
  filter(SUMM_VAR == "N" & COHORT == "GPC" & DATA_COVERAGE == "XWALK") %>%
  select(SITE,SUMM_CNT)
ggplot(dt1 %>% 
         inner_join(dt2 %>% select(-BENE_CNT),by="SITEID") %>%
         left_join(dt3_enc, by = c("SITEID"="SITE")) %>%
         replace_na(list(SUMM_CNT = 0)) %>%
         mutate(label=paste0(SITEID,"(",round(BENE_CNT_INCLD/1000),"K->",(SUMM_CNT/1000),"K)")),
       aes(x=RETENTION_RATE,y=DOB_MATCH_RATE))+
  geom_point(aes(size = BENE_CNT/1000),alpha = 0.8,color='orange')+
  geom_point(aes(size = BENE_CNT_INCLD/1000),alpha = 0.8,color='red')+
  geom_point(aes(size = SUMM_CNT/1000),alpha = 0.8,color='green')+
  scale_size(range = c(.1, 24), name="Population(K)")+
  geom_text_repel(aes(label=label))+
  theme(legend.position="none")

plot_cohort_overview<-function(){
  
  
  
}


plot_cohort_demo<-function(){
  
}

plot_cohort_geo<-function(){
  
  
}



# weight cohort
dt3_wt<-dt3 %>%
  filter(SUMM_VAR == "N" & SUMM_CAT == "N" & COHORT == "WT"  & DATA_COVERAGE != "ANY") %>%
  inner_join(dt3 %>%
               filter(SUMM_VAR == "N" & SUMM_CAT == "N" & COHORT == "WT" & DATA_COVERAGE == "ANY") %>%
               select(SITE, COHORT, SUMM_CNT) %>%
               rename(SUMM_DENOM = SUMM_CNT),
             by=c("SITE","COHORT")) %>%
  mutate(SUMM_PROP = SUMM_CNT/SUMM_DENOM) %>%
  mutate(label=paste0(round(SUMM_CNT/1000),"K(",round(SUMM_PROP*100),"%)"),
         page = case_when(SITE=="GPC"~"GPC",
                          TRUE ~ "Sites"))
  
ggplot(dt3 %>%
         filter(SUMM_VAR == "N" & SUMM_CAT == "N" & COHORT == "WT" & DATA_COVERAGE == "ANY") %>%
         mutate(label=paste0(round(SUMM_CNT/1000),"K"),
                SITE=paste0(letters[rank(-SUMM_CNT)],".",SITE)),
           aes(x=SITE,y=SUMM_CNT,group=DATA_COVERAGE))+
  geom_bar(aes(fill=DATA_COVERAGE),stat="identity",position="dodge")+
  geom_text(aes(label=label),position=position_dodge(width=1))+
  theme(axis.text.x=element_text(angle=60),
        text=element_text(face="bold",size=13),
        panel.background = element_rect(fill = NA, colour = NA),
        panel.grid.major.y = element_line(colour = "grey"))

ggplot(dt3_bc,aes(x=SITE,y=SUMM_CNT,group=DATA_COVERAGE))+
  geom_bar(aes(fill=DATA_COVERAGE),stat="identity",position="dodge")+
  geom_text(aes(label=label),position=position_dodge(width=1))+
  theme(axis.text.x=element_text(angle=60),
        text=element_text(face="bold",size=13),
        panel.background = element_rect(fill = NA, colour = NA),
        panel.grid.major.y = element_line(colour = "grey"))+
  facet_wrap(~page,ncol=1,scales="free")




# als cohort
dt3_als<-dt3 %>%
  filter(SUMM_VAR == "N" & SUMM_CAT == "N" & COHORT == "ALS"  & DATA_COVERAGE != "ANY") %>%
  inner_join(dt3 %>%
               filter(SUMM_VAR == "N" & SUMM_CAT == "N" & COHORT == "ALS" & DATA_COVERAGE == "ANY") %>%
               select(SITE, COHORT, SUMM_CNT) %>%
               rename(SUMM_DENOM = SUMM_CNT),
             by=c("SITE","COHORT")) %>%
  mutate(SUMM_PROP = SUMM_CNT/SUMM_DENOM) %>%
  mutate(label=paste0(round(SUMM_CNT/1000,1),"K(",round(SUMM_PROP*100),"%)"),
         page = case_when(SITE=="GPC"~"GPC",
                          TRUE ~ "Sites"))

ggplot(dt3 %>%
         filter(SUMM_VAR == "N" & SUMM_CAT == "N" & COHORT == "ALS" & DATA_COVERAGE == "ANY") %>%
         mutate(label=paste0(round(SUMM_CNT/1000,2),"K"),
                SITE=paste0(letters[rank(-SUMM_CNT)],".",SITE)),
       aes(x=SITE,y=SUMM_CNT,group=DATA_COVERAGE))+
  geom_bar(aes(fill=DATA_COVERAGE),stat="identity",position="dodge")+
  geom_text(aes(label=label),position=position_dodge(width=1))+
  theme(axis.text.x=element_text(angle=60),
        text=element_text(face="bold",size=13),
        panel.background = element_rect(fill = NA, colour = NA),
        panel.grid.major.y = element_line(colour = "grey"))

ggplot(dt3_als,aes(x=SITE,y=SUMM_CNT,group=DATA_COVERAGE))+
  geom_bar(aes(fill=DATA_COVERAGE),stat="identity",position="dodge")+
  geom_text(aes(label=label),position=position_dodge(width=1))+
  theme(axis.text.x=element_text(angle=60),
        text=element_text(face="bold",size=13),
        panel.background = element_rect(fill = NA, colour = NA),
        panel.grid.major.y = element_line(colour = "grey"))+
  facet_wrap(~page,ncol=1,scales="free")

# bc cohort
dt3_bc<-dt3 %>%
  filter(SUMM_VAR == "N" & SUMM_CAT == "N" & COHORT == "BC"  & DATA_COVERAGE != "ANY") %>%
  inner_join(dt3 %>%
               filter(SUMM_VAR == "N" & SUMM_CAT == "N" & COHORT == "BC" & DATA_COVERAGE == "ANY") %>%
               select(SITE, COHORT, SUMM_CNT) %>%
               rename(SUMM_DENOM = SUMM_CNT),
             by=c("SITE","COHORT")) %>%
  mutate(SUMM_PROP = SUMM_CNT/SUMM_DENOM) %>%
  mutate(label=paste0(round(SUMM_CNT/1000),"K(",round(SUMM_PROP*100),"%)"),
         page = case_when(SITE=="GPC"~"GPC",
                          TRUE ~ "Sites"))

ggplot(dt3 %>%
         filter(SUMM_VAR == "N" & SUMM_CAT == "N" & COHORT == "BC" & DATA_COVERAGE == "ANY") %>%
         mutate(label=paste0(round(SUMM_CNT/1000),"K"),
                SITE=paste0(letters[rank(-SUMM_CNT)],".",SITE)),
       aes(x=SITE,y=SUMM_CNT,group=DATA_COVERAGE))+
  geom_bar(aes(fill=DATA_COVERAGE),stat="identity",position="dodge")+
  geom_text(aes(label=label),position=position_dodge(width=1))+
  theme(axis.text.x=element_text(angle=60),
        text=element_text(face="bold",size=13),
        panel.background = element_rect(fill = NA, colour = NA),
        panel.grid.major.y = element_line(colour = "grey"))

ggplot(dt3_wt,aes(x=SITE,y=SUMM_CNT,group=DATA_COVERAGE))+
  geom_bar(aes(fill=DATA_COVERAGE),stat="identity",position="dodge")+
  geom_text(aes(label=label),position=position_dodge(width=1))+
  theme(axis.text.x=element_text(angle=60),
        text=element_text(face="bold",size=13),
        panel.background = element_rect(fill = NA, colour = NA),
        panel.grid.major.y = element_line(colour = "grey"))+
  facet_wrap(~page,ncol=1,scales="free")




