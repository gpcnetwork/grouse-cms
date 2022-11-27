rm(list=ls())
setwd("C:/Users/sxing/projects/repos/GROUSE")

source("./R/util.R")
require_libraries(c( "dplyr"
                    ,"tidyr"
                    ,"magrittr"
                    ,"stringr"
                    ,"ggplot2"
                    ,"ggpubr"
                    ,"ggrepel"
                    ,"kableExtra"
                    ,"magick"
))

####Population Characteristics####
dem_tbl<-readRDS("./data/Demo_Overview.rda")

#manage table layout
pat_tbl_wide<-dem_tbl$pat_tbl %>%
  #need to preserve age-group order 
  mutate(cat=case_when(cat=="0-4"~"1.0-4",
                       cat=="5-14"~"2.5-14",
                       cat=="15-21"~"3.15-21",
                       cat=="22-64"~"4.22-64",
                       cat=="65+"~"5.65+",
                       cat=="Missing"~"6.Missing",
                       TRUE~cat)) %>%
  #want to place "GPC" in the first row and "Patients" in the first column
  mutate(site=case_when(site=="GPC"~" GPC",
                        TRUE ~ site),
         var=case_when(var=="Patients"~" Patients",
                       TRUE ~ var),
         cat=case_when(var=="Patients"~" Patients",
                       TRUE ~ cat)) %>%
  #make sure when pivot the data, columns are in proper order
  mutate(ord=paste0(dense_rank(var),letters[dense_rank(cat)])) %>%
  unite(colnm,c("ord","var","cat"),sep=";") %>%
  unite(entry,c("val","perc"),sep="     (",na.rm=T) %>%
  mutate(entry=case_when(grepl("\\(",entry)~paste0(entry,")"),
                         TRUE~entry)) %>%
  select(colnm,entry,site) %>%
  #table pivot
  spread(colnm,entry)

colnames(pat_tbl_wide)<-gsub(".*(;|\\.)","",colnames(pat_tbl_wide))
  
kable(pat_tbl_wide) %>%
  # kable_paper(bootstrap_options="striped") %>%
  kable_classic_2(bootstrap_options="striped") %>%
  add_header_above(c(" "=1, 
                     "N"=1,
                     "Age"=2, 
                     "Age Group"=6,
                     "Hispanic"=3,
                     "Race"=3,
                     "Sex"=3)) %>%
  save_kable("./img/DemoTbl.png")

obspat_tbl<-dem_tbl$obspat_tbl %>%
  bind_rows(dem_tbl$pat_tbl %>%
              filter(var=="Patients") %>%
              select(site,var,val) %>%
              mutate(val_type="cnt")) %>%
  mutate(val=case_when(val_type=="perc"~paste0(val,"%"),
                       TRUE ~ as.character(val)),
         site=case_when(site=="GPC"~" GPC",
                        TRUE ~ site)) %>%
  select(-val_type) %>%
  mutate(var=recode(var,
                    "Patients"="1.Patients",
                    "pat_obs"="2.Observational Patients",
                    "pat_pheno"="3.Computational Patients",
                    "pat_trial"="4.Patients eligible for Trials",
                    "pat_dx"="5.Patients with Diagnosis (%)",
                    "pat_px"="6.Patients with Procedures (%)")) %>%
  spread(var,val)

kable(obspat_tbl) %>%
  # kable_paper(bootstrap_options="striped") %>%
  kable_classic_2(bootstrap_options="striped") %>%
  save_kable("./img/DemoTbl2.png")


####Participation Metrics####
contr<-readRDS("./data/Contr_Proj.rda")

#adaptable
ggplot(contr[[1]]$site %>%
         filter(rpt_ord==4) %>%
         select(site,enroll_rate,success_query) %>%
         mutate(success_query=as.numeric(gsub("\\%","",success_query)),
                enroll_rate=as.numeric(enroll_rate),
                rk=letters[dense_rank(-enroll_rate)]) %>%
         unite(site,c("rk","site"),sep=".") %>%
         mutate(success_query_rescale=((success_query-0)/(max(success_query)-0)*(max(enroll_rate)-min(enroll_rate))+min(enroll_rate))),
       aes(x=site))+
  geom_bar(aes(y=enroll_rate),stat="identity",fill="grey")+
  geom_line(aes(y=success_query_rescale,group=1),size=1,linetype=2,color="blue")+
  geom_point(aes(y=success_query_rescale))+
  scale_y_continuous(name = "Enrollment Rate", 
                     sec.axis = sec_axis(~(. - 15.616)/(24.2-15.616)*37+63, name = "Successful Query %", 
                                         labels = function(b) { paste0(round(b, 0), "%")})) +
  labs(x="")+
  theme(text=element_text(face="bold",size=15),
        axis.text.x = element_text(angle = 75),
        axis.title.x = element_blank(),
        # legend.position = "bottom",
        # legend.margin=margin(0,0,0,0),
        # legend.box.margin=margin(-60,-30,0,0)
        )

ggsave(file="./img/Adaptable.png",
       width=5, height=5.5)

#preventable
# contr[[2]]$gpc %>% View
# ggplot(contr[[2]]$site %>%
#          filter(rpt_ord==4&day2active!="N/A") %>%
#          select(site,enroll_rate,success_query,day2active) %>%
#          mutate(day2active=as.numeric(day2active),
#                 rk=letters[dense_rank(day2active)]) %>%
#          unite(site,c("rk","site"),sep="."),
#        aes(x=site))+
#   geom_bar(aes(y=day2active),stat="identity")+
#   geom_hline(yintercept = 164.9,linetype=2)+
#   labs(x="",y="Day to Site Activation")+
#   theme(axis.text.x = element_text(angle = 45),
#         text=element_text(face="bold"),
#         legend.position = "bottom",
#         legend.margin=margin(0,0,0,0),
#         legend.box.margin=margin(-50,-30,0,0))
# 
# ggsave(file="./img/Preventable.png",
#        width=6, height=5)

#front_door
fdoor<-contr[[8]] %>% 
  mutate(cnt_den=as.numeric(cnt_den),
         cnt_num=as.numeric(cnt_num)) %>%
  filter(rpt_ord==4) %>% #report is cumulative
  select(site,rpt_ord,cnt_den,cnt_num,percentage) %>%
  mutate(proj="Regular Front Door Requests",
         covid="non-COVID") %>%
  bind_rows(contr[[7]]$covid %>%
              select(site,rpt_ord,cnt_den,cnt_num,percentage,proj) %>%
              mutate(covid="COVID")) %>%
  bind_rows(contr[[7]]$front_door_covid %>%
              group_by(site) %>%
              summarise(rpt_ord=min(rpt_ord),
                        cnt_num=sum(as.numeric(col2=="Yes")),
                        .groups="drop") %>%
              mutate(cnt_den=2,
                     proj="Network COVID-19 CDM Queries",
                     covid="COVID") %>%
              mutate(percentage=cnt_num/cnt_den) %>%
              select(site,rpt_ord,cnt_den,cnt_num,percentage,proj,covid)) %>%
  mutate(label=paste0(cnt_num,"(",percentage,"%)"))

fdoor %>%
  group_by(proj,covid) %>% # unique query
  summarise(N=max(cnt_den,na.rm=T),
            n_max=max(cnt_num,na.rm=T),
            n_mean=mean(cnt_num,na.rm=T),
            n_sd=sd(cnt_num,na.rm=T),
            n_sum=sum(cnt_num),
            p_max=max(percentage,na.rm=T),
            p_mean=mean(percentage,na.rm=T),
            p_sd=sd(percentage,na.rm=T),
            .groups="drop") %>%
  group_by(covid) %>%
  summarise(N=sum(N), 
            n=sum(n_max),
            n2=sum(n_mean),
            n_dm=sum(n_sum),
            n_dm_query=floor(n_dm/N),
            .groups="drop") %>%
  View

req_N<-51
ggplot(fdoor %>% 
         group_by(site,covid,) %>%
         summarise(cnt_num=sum(cnt_num,na.rm=F),
                   cnt_den=sum(cnt_den,na.rm=F),
                   # rapid_alpha=case_when(min(rpt_ord)<3~1,
                   #                       TRUE~0.8),
                   .groups="keep") %>%
         arrange(as.numeric(covid=="COVID")) %>%
         ungroup %>%
         group_by(site) %>%
         mutate(cnt_num_ref=cumsum(cnt_num),
                cnt_num_cum=sum(cnt_num)) %>%
         ungroup %>%
         mutate(rk=letters[dense_rank(-cnt_num_cum)]) %>%
         unite(site,c("rk","site"),sep="."), 
       aes(x=site,y=cnt_num))+
  geom_bar(aes(fill=covid),position="stack",stat="identity")+
  geom_text(aes(y=cnt_num_ref,label=cnt_num),fontface="bold",size=5)+
  scale_y_continuous("Number of Completed Queries", 
                     sec.axis = sec_axis(~ . /req_N, 
                                         name = "Percentage")) +
  geom_hline(yintercept = 0.6*req_N,linetype=2)+
  labs(x="",fill="COVID-related")+
  theme(text=element_text(face="bold",size=15),
        axis.text.x = element_text(angle = 75),
        legend.position = "bottom",
        legend.margin=margin(0,0,0,0),
        legend.box.margin=margin(-60,-30,0,0))
  
ggsave(file="./img/FrontDoorComplete.png",
       width=4.5, height=5)

#hero
ggplot(contr[[3]]$site %>%
         filter(rpt_ord==4&part_ind == "Yes") %>%
         mutate(enroll_rate=as.numeric(enroll_rate),
                rk=letters[dense_rank(-enroll_rate)]) %>%
         unite(site,c("rk","site"),sep="."),
       aes(x=site,y=enroll_rate))+
  geom_bar(stat="identity")+
  labs(x="",y="Enrollment Rate (/month)")+
  theme(axis.text.x = element_text(angle =45),
        axis.title.x=element_blank(),
        text=element_text(face="bold"))

ggsave(file="./img/HERO_HCQ1.png",
       width=5, height=2.5)


ggplot(contr[[3]]$site %>%
         filter(rpt_ord==4&part_ind == "Yes") %>%
         mutate(day2active=as.numeric(day2active),
                day2enroll=as.numeric(day2enroll),
                rk=letters[dense_rank(day2enroll)]) %>%
         unite(site,c("rk","site"),sep=".") %>%
         select(site,day2active,day2enroll) %>%
         gather(event,days,-site) %>%
         mutate(event=recode(event,
                             "day2active"="days to site activation",
                             "day2enroll"="days to first patient enrolled")),
       aes(x=site,y=days))+
  geom_line(aes(group=event,linetype=event),size=1)+ 
  geom_point(aes(shape=event),size=3)+
  labs(x="",y="Days to Enrollment Milestones",
       linetype="Milestone",shape="Milestone")+
  guides(linetype=guide_legend(nrow=2,byrow=TRUE))+
  theme(axis.text.x = element_text(angle = 45),
        text=element_text(face="bold",size=15),
        legend.position = "bottom",
        legend.margin=margin(0,0,0,0),
        legend.box.margin=margin(-40,-30,0,0))

ggsave(file="./img/HERO_HCQ2.png",
       width=5, height=5)


####Quality Metrics####
qc_matrix<-readRDS("./data/QC_Matrix.rda")
plots<-list()
for(idx in c(1:18,"a","b","c","d")){
  dat<-qc_matrix %>% 
    filter(token_index==paste0(idx,")")) 
  
  if(idx %in% c(1,6:15)){
    dat %<>% 
      mutate(val=gsub("%","",col2)) %>%
      mutate(percentage=as.numeric(val))
    
    plt<-ggplot(dat,aes(x=rpt_ord,y=percentage))+
      geom_line(aes(group=site,color=site)) +
      geom_point() +
      geom_smooth(method='lm',formula="y~x",linetype=2) +
      stat_regline_equation(label.y = 110)
    
  }else if(idx %in% c(2,5)){
    dat %<>% 
      mutate(val=gsub(",","",col2)) %>%
      mutate(count=as.numeric(val))
    
    plt<-ggplot(dat,aes(x=rpt_ord,y=count))+
      geom_line(aes(group=site,color=site)) +
      geom_point() +
      geom_smooth(method='lm',formula="y~x",linetype=2) +
      stat_regline_equation(label.y = max(dat$count)+1)
    
  }else if(idx %in% c(3:4)){
    dat %<>%
      mutate(val=as.numeric((col2=="Yes")))
    
    plt<-ggplot(dat,aes(x=rpt_ord))+
      geom_bar(aes(fill=col2))
    
  }else if(idx %in% c(16:18)){
    dat %<>%
      mutate(val=gsub("((Patients, )|(Records, )|(Codes, ))+","",col2)) %>%
      separate(val,c("Patients_Codes","Records"),sep=", ") %>%
      gather(var,val,-token_id,-token_index,-col1,-col2,-site,-rpt_date,-rpt_ord,-data_latency_goal) %>%
      mutate(col2=var) %>% select(-var)
    
    plt<-ggplot(dat,aes(x=rpt_ord))+
      geom_bar(aes(fill=val,alpha=col2),position="dodge")+
      scale_alpha_manual(values=c("Patients_Codes" = 1, "Records" = 0.4))
    
  }else{
    dat %<>%
      mutate(data_latency=as.Date(col2,format="%m/%d/%Y")-as.Date(data_latency_goal,format="%Y-%m-%d"),
             data_latency=round(as.numeric(data_latency)/30,1))
    
    plt<-ggplot(dat,aes(x=rpt_ord,y=data_latency))+
      geom_line(aes(group=site,color=site)) +
      geom_point() +
      geom_smooth(method='lm',formula="y~x",linetype=2)
  }
  
  plots[[idx]]<-plt + 
    theme(legend.position = "bottom")+
    labs(x=paste0(idx,")"),
         # y=
         # subtitle=dat$col1[1]
         subtitle=paste0(substr(dat$col1[1],1,170),"-\n",
                         substr(dat$col1[1],171,nchar(dat$col1[1])))
         ) 
}

plots[[1]]
plots[[2]]
plots[[3]]
plots[[4]]
plots[[5]]
plots[[6]]

plots[[7]] #send to russ
ggsave(file="./img/Enc_w_Procedure.png",
       width=10, height=6)


plots[[8]] #send to russ
ggsave(file="./img/Enc_w_Prescribing.png",
       width=10, height=6)


plots[[9]] #send to russ
ggsave(file="./img/Enc_w_Lab.png",
       width=10, height=6)

plots[[10]]
# plots[[11]]
plots[[12]]

plots[[13]]
plots[[14]]

# plots[[15]]
plots[[16]]
plots[[17]]
plots[[18]]

plots[[19]]
plots[[20]]
plots[[21]]
plots[[22]]


#refresh approval by deadline (01/19 - 01/21)
dat<-qc_matrix %>% 
  filter(token_index==paste0(1,")")) %>% 
  mutate(val=gsub("%","",col2)) %>%
  mutate(percentage=as.numeric(val)) %>%
  filter(rpt_ord==4)

#quality checks
dat<-qc_matrix %>% 
  filter(token_index==paste0(2,")")) %>% 
  mutate(val=gsub("%","",col2)) %>%
  mutate(percentage=as.numeric(val)) %>%
  filter(rpt_ord==4)

# concept mappings
dat<-qc_matrix %>% 
  filter(token_index==paste0(10,")")) %>% 
  mutate(val=gsub("%","",col2)) %>%
  mutate(percentage=as.numeric(val)) %>%
  bind_rows(qc_matrix %>% 
              filter(token_index==paste0(11,")")) %>% 
              mutate(val=gsub("%","",col2)) %>%
              mutate(percentage=as.numeric(val))) %>%
  bind_rows(qc_matrix %>% 
              filter(token_index==paste0(12,")")) %>% 
              mutate(val=gsub("%","",col2)) %>%
              mutate(percentage=as.numeric(val))) %>%
  filter(rpt_ord==4)

# common concepts
dat<-qc_matrix %>% 
  filter(token_index==paste0(13,")")) %>% 
  mutate(val=gsub("%","",col2)) %>%
  mutate(percentage=as.numeric(val)) %>%
  filter(rpt_ord==4)

dat<-qc_matrix %>% 
  filter(token_index==paste0(14,")")) %>% 
  mutate(val=gsub("%","",col2)) %>%
  mutate(percentage=as.numeric(val)) %>%
  filter(rpt_ord==4)


# meaningful encounters
dat<-qc_matrix %>% 
  filter(token_index==paste0(6,")")) %>% 
  mutate(val=gsub("%","",col2)) %>%
  mutate(percentage=as.numeric(val)) %>%
  bind_rows(qc_matrix %>% 
              filter(token_index==paste0(7,")")) %>% 
              mutate(val=gsub("%","",col2)) %>%
              mutate(percentage=as.numeric(val))) %>%
  bind_rows(qc_matrix %>% 
              filter(token_index==paste0(8,")")) %>% 
              mutate(val=gsub("%","",col2)) %>%
              mutate(percentage=as.numeric(val))) %>%
  bind_rows(qc_matrix %>% 
              filter(token_index==paste0(9,")")) %>% 
              mutate(val=gsub("%","",col2)) %>%
              mutate(percentage=as.numeric(val))) %>%
  group_by(rpt_ord,token_index) %>%
  mutate(perc_med=median(percentage),
         perc_q1=quantile(percentage,0.25),
         perc_q3=quantile(percentage,0.75)) %>%
  ungroup %>%
  mutate(token_label=recode(token_index,
                            "6)"="a.Encounter\n+Diagnosis",
                            "7)"="b.Encounter\n+Procedure",
                            "8)"="c.Encounter\n+Prescribing",
                            "9)"="d.Encounter\n+Lab")) %>%
  group_by(token_index) %>%
  mutate(tier=cut(percentage,breaks=quantile(percentage,c(0,0.25,0.75,1)),
                  include.lowest=TRUE, labels=FALSE),
         mu_ind=case_when(site=="MISSOURI"~2,
                          TRUE~1)) %>%
  ungroup %>%
  filter(rpt_ord %in% c(1,3,4)) %>%
  mutate(rpt_yr=recode(rpt_ord,
                       "1"="2018",
                       "3"="2019",
                       "4"="2020"))

  
ggplot(dat %>% 
         select(token_index,token_label,site,percentage,rpt_yr,tier,mu_ind),
       aes(x=rpt_yr,y=percentage)) +
  geom_line(aes(group=site,alpha=mu_ind),size=1)+
  geom_point(aes(color=as.factor(tier)),size=3)+
  labs(x="Calendar Year",y="Percentage of Patients")+
  theme(axis.text.x = element_text(angle = 90),
        text=element_text(face="bold"),
        # legend.position = "bottom",
        # legend.margin=margin(0,0,0,0),
        # legend.box.margin=margin(-40,-30,0,0)
        legend.position = "none"
        )+
  facet_grid(~token_label)

ggsave(file="./img/MU_QC_Recovery.png",
       width=5, height=3)
