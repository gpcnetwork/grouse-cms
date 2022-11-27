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

## load data
dat<-read.csv("./data/site_participation.csv",stringsAsFactors = F) %>%
  mutate(state=tolower(state))

## plot
us<-map_data("state")
long_lim<-c(min(us$long),max(us$long))
lat_lim<-c(min(us$lat),max(us$lat))
stannote<-data.frame(
  long=state.center$x,
  lat=state.center$y,
  state_abb=state.abb)

dat_st<-us %>%
  left_join(dat %>% select(id,state) %>%
              separate(state,c("st1","st2","st3","st4"),sep=",") %>%
              gather(var,state,-id) %>% select(-id) %>%
              filter(!is.na(state)) %>% mutate(state=trimws(state,which="both")) %>%
              mutate(new_ind = case_when(state %in% c("illinois","nevada","wyoming","idaho","michigan") ~ 2, 
                                         TRUE ~ 1),
                     gpc_ind = 1),
            by=c("region"="state"))

ggplot(dat_st,aes(x=long,y=lat))+
  geom_polygon(aes(fill=as.factor(new_ind),group=group,alpha=gpc_ind,color='grey')) +
  geom_text(data=stannote,aes(label=state_abb))+
  guides(fill="none", alpha="none", color="none")+
  theme(text=element_text(face="bold",size=20),
        panel.background = element_rect(fill = NA, colour = NA),
        panel.grid.major = element_line(colour = NA))

dat_task<-dat %>%
  select(id,site_full,site_short,s1,s2,s3,s4,s5,s6,s7) %>%
  gather(var,val,-id,-site_full,-site_short) %>%
  group_by(id) %>% arrange(var) %>% mutate(val_cum=cumsum(val),val_max=max(val_cum)) %>%
  mutate(val_cum_fac = as.factor(-val_cum)) %>%
  ungroup
  
step_labels=c("activate-AWS-account", #s1
              "IRB-reliance", #s2
              "GROUSE-DUA-execution", #s3
              "Upload-CDM-datamart", #s4
              "Submit-finder-file", #s5
              "Include-in-crosswalk", #s6
              "Submit-PATID-mapping" #s7
              )
ggplot(dat_task,aes(x=reorder(site_full,desc(val_max)),fill=val_cum_fac)) +
  geom_bar(aes(y=factor(val)),stat="identity")+
  labs(x="GPC site",y="Data Refresh Progress")+
  scale_y_discrete(limits = 1:length(step_labels),
                   breaks = 1:length(step_labels), 
                   labels=step_labels) +
  theme(axis.text.x=element_text(angle=60),
        text=element_text(face="bold",size=18),
        panel.background = element_rect(fill = NA, colour = NA),
        panel.grid.major.y = element_line(colour = "grey"))+
  guides(fill="none")




               