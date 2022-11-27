## extract standardized PCORnet reports
# - make sure to sync OneDrive with the shared folder where the raw reports are stored:
#   ...\University of Missouri\NextGen-BMI - Ogrp - Documents\General\GPC\NetworkProgressRevisedReports2020
#   ...\University of Missouri\NextGen-BMI - Ogrp - Documents\General\GPC\PCORnet 3.0\Site submission
rm(list=ls())
setwd("C:/repo/GROUSE")

pacman::p_load(tidyverse,
               magrittr,
               pdftools,
               devtools,
               ggrepel)

# source util functions
source_url("https://raw.github.com/sxinger/utils/master/plot_util.R")

####Setup####
path_to_folder<-file.path("C:/Users/xsm7f",
                          "University of Missouri",
                          "NextGen-BMI - Ogrp - Documents",
                          "GPC_ADMIN",
                          "GPC EDC Reports",
                          "2021-Q4")
file_lst<-list.files(path = path_to_folder)
file_lst<-file_lst[grepl("(GPC)+.*(pdf)",file_lst)][-1] #first report is duplicated

#---------------------------------------------------------------
#Parse Network Progress Report
#---------------------------------------------------------------
#report paging seem to change all the time
pg_rg<-data.frame(rpt_ord=1:4,
                  rpt_date=c("March 25, 2020","September 1, 2020","December 3, 2020","March 4, 2021"),
                  pg_st_end=c("13,35","12,33","17,40","16,38"),
                  data_latency_goal=c("2019-10-31","2020-05-31","2020-07-31","2020-12-31"),
                  stringsAsFactors = F) %>%
  separate(pg_st_end,c("pg_start","pg_end"),sep=",") %>%
  mutate(pg_start=as.numeric(pg_start),
         pg_end=as.numeric(pg_end))

#start parsing
file_parse_tbl<-list()
for(i in seq_along(file_lst)){
  file_i<-file.path(path_to_folder, file_lst[i])
  file_parse_lst<-pdf_text(file_i) %>% strsplit("\n")
  
  #parse multiple tables
  file_parse_tbl[[i]]<-list()
  
  #parse report date
  file_parse_tbl[[i]][[1]]<-file_parse_lst[[1]][grepl("^\\s+(Date of Report)+",file_parse_lst[[1]])]
  file_parse_tbl[[i]][[1]]<-trimws(gsub(".*\\:","",file_parse_tbl[[i]][[1]]),"both")
  
  ## tables from each page
  pg_range<-pg_rg %>% 
    filter(rpt_date==file_parse_tbl[[i]][[1]])
  for(j in 2:(pg_range$pg_start-1)){
    #existing package "tabulizer" can't always detect a table structure
    # file_parse_tbl[[i]][[j]]<-extract_tables(file_i,
    #                                          pages=j,
    #                                          guess = TRUE)
    
    file_parse_tbl[[i]][[j]]<-parse_proci_rpt(file_parse_lst[[j]],
                                              template="NPR_MultiColMultiLine")
  }
  
  for(j in pg_range$pg_start:pg_range$pg_end){
    file_parse_tbl[[i]][[j]]<-parse_proci_rpt(file_parse_lst[[j]],
                                              template="NPR_2CollMultiLine")
  }
}

# saveRDS(file_parse_tbl,file="./data/scrapped_report.rda")
#---------------------------------------------------------------

####Stack Table across reports####
file_parse_tbl<-readRDS("./data/scrapped_report.rda")

#----QC----------------
qc_matrix<-c()
for(i in seq_along(file_lst)){
  pg_range<-pg_rg %>% 
    filter(rpt_date==file_parse_tbl[[i]][[1]])
  
  for(j in pg_range$pg_start:pg_range$pg_end){
    
    if(sum(file_parse_tbl[[i]][[j]]$token_id=="token_3",na.rm=T)>0){
      #initial page
      dat_i<-file_parse_tbl[[i]][[j]] %>%
        filter(token_id %in% c("token_3","token_5","token_6")) %>%
        mutate(site=col1[.$token_id=="token_3"]) %>%
        filter(token_id != "token_3") %>% 
        select(-rowid)
    }else{
      #subsequent page
      dat_i<-file_parse_tbl[[i]][[j]] %>%
        filter(token_id %in% c("token_5","token_6")) %>% 
        select(-rowid)
    }
    
    qc_matrix %<>%
      bind_rows(dat_i %>%
                  mutate(rpt_date=unlist(file_parse_tbl[[i]][[1]])))
  }
}

qc_matrix %<>%
  mutate(col1_cp=col1,
         col1=case_when(col1 %in% c(""," ") ~ gsub(",.*","",col2),
                        TRUE ~ col1),
         col1=trimws(col1),
         col2=case_when(col1_cp %in% c(""," ") ~ gsub(".*,","",col2),
                        TRUE ~ col2),
         col2=trimws(col2)) %>%
  select(-col1_cp) %>%
  fill(site,.direction="down") %>%
  #manual clean
  mutate(col2=case_when(col2==""&token_index=="17)"&rpt_date=="March 4, 2021"~"Codes, No, Records, No",
                        col2==""&token_index=="15)"&rpt_date=="March 4, 2021"~"Codes, No, Records, No",
                        col2==""&token_index=="16)"&rpt_date=="December 3, 2020"~"Codes, No, Records, No",
                        col2==""&token_index=="18)"&rpt_date=="December 3, 2020"~"Codes, No, Records, Yes",
                        TRUE ~ col2),
         site=case_when(toupper(site)=="IU REG" ~ "IU",
                        TRUE ~ toupper(site))) %>%
  left_join(pg_rg %>% select(rpt_date,rpt_ord,data_latency_goal),by="rpt_date") %>%
  #Q11 removed in the last report, need to remap the questions
  mutate(token_index2=case_when(token_id == "token_5" ~ as.numeric(gsub(")","",token_index)))) %>%
  mutate(token_index2=case_when(token_id == "token_5"&rpt_ord==4&token_index2>=11 ~ token_index2+1,
                                TRUE ~ token_index2),
         token_index=case_when(token_id == "token_5" ~ paste0(token_index2,")"),
                               TRUE ~ token_index)) %>%
  select(-token_index2)

saveRDS(qc_matrix,file="./data/QC_Matrix.rda")

#----CDA latency------------------
cda_dur<-c()
for(i in seq_along(file_lst)){ 
  if(unlist(file_parse_tbl[[i]][[1]])=="March 25, 2020"){
    cda_dur %<>%
      bind_rows(cbind(rpt_date=unlist(file_parse_tbl[[i]][[1]]),
                      file_parse_tbl[[i]][[2]] %>%
                        filter(!is.na(col3)) %>%
                        select(col1,col3) %>% #based on visual inspection
                        rename(site=col1,days_to_sign=col3) %>%
                        mutate(agreement="DSA",
                               ref_avg=55)
      )) %>%
      bind_rows(cbind(rpt_date=unlist(file_parse_tbl[[i]][[1]]),
                      file_parse_tbl[[i]][[2]] %>%
                        filter(!is.na(col5)) %>%
                        select(col1,col5) %>%
                        rename(site=col1,days_to_sign=col5)%>%
                        mutate(agreement="DSA",
                               ref_avg=30)
      )) %>%
      bind_rows(cbind(rpt_date=unlist(file_parse_tbl[[i]][[1]]),
                      file_parse_tbl[[i]][[2]] %>%
                        filter(!is.na(col7)) %>%
                        select(col1,col7) %>%
                        rename(site=col1,days_to_sign=col7)%>%
                        mutate(agreement="CDA",
                               ref_avg=41)
      ))
  }else{
    cda_dur %<>%
      bind_rows(cbind(rpt_date=unlist(file_parse_tbl[[i]][[1]]),
                      file_parse_tbl[[i]][[2]] %>%
                        filter(!is.na(col3)) %>%
                        select(col1,col3) %>% #based on visual inspection
                        rename(site=col1,days_to_sign=col3) %>%
                        mutate(agreement="CDA",
                               ref_avg=case_when(unlist(file_parse_tbl[[i]][[1]])=="September 1, 2020"~64,
                                                 unlist(file_parse_tbl[[i]][[1]])=="December 3, 2020"~67
                                                 ))
      ))
  }
}

saveRDS(cda_dur,file="./data/Agmt_Dur.rda")


#----Contribution--------
research_contr<-c()
front_door_query<-c()
for(i in seq_along(file_lst)){
  pg_range<-pg_rg %>% 
    filter(rpt_date==file_parse_tbl[[i]][[1]])
  
  for(j in 2:pg_range$pg_end){
    
    if(sum(file_parse_tbl[[i]][[j]]$token_id=="token_2",na.rm=T)>0){
      #study participation stats page
      dat_i<-file_parse_tbl[[i]][[j]] %>%
        filter(token_id %in% c("token_2","token_3","token_4")) 
      
      if(sum(file_parse_tbl[[i]][[j]]$token_id=="token_2",na.rm=T)>1){
        dat_i %<>%
          mutate(proj=col2[.$token_id=="token_2"][2])
      }else{
        dat_i %<>%
          mutate(proj=col1[.$token_id=="token_2"]) 
      }
      
      dat_i %<>%
        filter(token_id != "token_2"&!is.na(col2)&!col2 %in% c(""," ")) %>%
        select(all_of(intersect(colnames(dat_i),
                                c('token_id','token_index','col1','col2','col3','col4',"col5","col6",'proj'))))
      
      research_contr %<>%
        bind_rows(dat_i %>%
                    mutate(rpt_date=unlist(file_parse_tbl[[i]][[1]])))
      
    }else if(sum(file_parse_tbl[[i]][[j]]$token_index %in% c("B.","C."),na.rm=T)>0){
      #front-door query completion data
      dat_i<-file_parse_tbl[[i]][[j]] %>%
        filter(token_index %in% c("B.","C.")) %>%
        #extract "Yes"
        mutate(col2=case_when(token_index=="C." ~ str_extract(col1,"(Yes)+"),
                              !is.na(col2) ~ col2),
               col1=case_when(token_index=="C." ~ gsub("(Yes)+","",col1),
                              !is.na(col2) ~ col1)) %>%
        select(-rowid)
      
      #identify site
      if(sum(file_parse_tbl[[i]][[j]]$token_id=="token_3",na.rm=T)==0){
        site<-file_parse_tbl[[i]][[j-1]]$col1[file_parse_tbl[[i]][[j-1]]$token_id=="token_3"][1]
      }else{
        site<-file_parse_tbl[[i]][[j]]$col1[file_parse_tbl[[i]][[j]]$token_id=="token_3"][1]
      }
      
      #manual curation - when table got cut over pages
      if(file_parse_tbl[[i]][[1]]=="December 3, 2020"){
        dat_i %<>%
          mutate(col2=case_when(token_index=="B."&site=="Marshfield" ~ "95% (38 of 40)",
                                token_index=="B."&site=="MCW" ~ "100% (40 of 40)",
                                token_index=="B."&site=="UNMC" ~ "90% (36 of 40)",
                                token_index=="B."&site=="UTSWMC" ~ "95% (38 of 40)",
                                token_index=="B."&site=="Utah" ~ "100% (40 of 40)",
                                TRUE ~ col2))
      }
      
      if(file_parse_tbl[[i]][[1]]=="March 4, 2021"){
        dat_i %<>%
          mutate(col2=case_when(token_index=="B."&site=="Utah" ~ "43 of 43 (100%)",
                                TRUE ~ col2))
      }
       
      front_door_query %<>%
        bind_rows(dat_i %>%
                    mutate(col2=gsub(";;.*","",col2)) %>%
                    filter(!is.na(col2)&!col2 %in% c(""," ")) %>%
                    mutate(site=gsub(" .*","",site),
                           rpt_date=unlist(file_parse_tbl[[i]][[1]])))
      
    }else{
      next
    }
  }
}

#one project per table
#--adaptable
adaptable<-research_contr %>%
  filter(grepl("ADAPTABLE",toupper(proj))) %>%
  select(rpt_date,col1,col2,col3,col4,col5,col6) %>%
  left_join(pg_rg %>% select(rpt_date,rpt_ord),by="rpt_date")

adaptable_lst<-list(gpc=adaptable %>% filter(col1=="GPC") %>%
                      rename(part_perc=col2,part_cnt=col3,
                             avg_day2active=col4,
                             avg_day2enroll=col5,
                             avg_enroll_rate=col6),
                    site=adaptable %>% filter(col1!="GPC") %>%
                      rename(site=col1,
                             part_ind=col2,
                             day2active=col3,
                             day2enroll=col4,
                             enroll_rate=col5,
                             success_query=col6))

#--preventable
preventable<-research_contr %>%
  filter(grepl("PREVENTABLE",toupper(proj))) %>%
  select(rpt_date,col1,col2,col3,col4,col5,col6) %>%
  left_join(pg_rg %>% select(rpt_date,rpt_ord),by="rpt_date")

preventable_lst<-list(gpc=preventable %>% filter(col1=="GPC") %>%
                      rename(part_perc=col2,part_cnt=col3,
                             avg_day2active=col4,
                             avg_day2enroll=col5,
                             avg_enroll_rate=col6),
                    site=preventable %>% filter(col1!="GPC") %>%
                      rename(site=col1,
                             part_ind=col2,
                             day2active=col3,
                             day2enroll=col4,
                             enroll_rate=col5,
                             success_query=col6))



#--hero
hero<-research_contr %>%
  filter(grepl("HERO-HCQ",toupper(proj))) %>%
  select(rpt_date,col1,col2,col3,col4,col5,col6) %>%
  left_join(pg_rg %>% select(rpt_date,rpt_ord),by="rpt_date")

hero_lst<-list(gpc=hero %>% filter(col1=="GPC") %>%
                 rename(part_perc=col2,part_cnt=col3,
                        avg_day2active=col4,
                        avg_day2enroll=col5,
                        avg_enroll_rate=col6),
               site=hero %>% filter(col1!="GPC") %>%
                 rename(site=col1,
                        part_ind=col2,
                        day2active=col3,
                        day2enroll=col4,
                        enroll_rate=col5) %>%
                 select(-col6))

#--bp track
bp_ctrl<-research_contr %>%
  filter(grepl("BP",toupper(proj))) %>%
  select(rpt_date,proj,col1,col2,col3,col4) %>%
  left_join(pg_rg %>% select(rpt_date,rpt_ord),by="rpt_date")

bp_lst<-list(gpc=bp_ctrl %>% filter(col1=="GPC") %>%
                 rename(part_perc=col2,part_cnt=col3,
                        avg_day2active=col4),
               site=bp_ctrl %>% filter(col1!="GPC") %>%
                 rename(site=col1,
                        part_ind=col2,
                        day2active=col3) %>%
               select(-col4))

#--bariatric
bariatric<-research_contr %>%
  filter(grepl("BARIATRIC",toupper(proj))) %>%
  select(rpt_date,col1,col2,col3,col4) %>%
  left_join(pg_rg %>% select(rpt_date,rpt_ord),by="rpt_date")

bariatric_lst<-list(gpc=bariatric %>% filter(col1=="GPC") %>%
                      rename(part_perc=col2,part_cnt=col3,
                             avg_day2active=col4),
                    site=bariatric %>% filter(col1!="GPC") %>%
                      rename(site=col1,
                             part_ind=col2,
                             day2active=col3))

#--childhood
childhood_abx<-research_contr %>%
  filter(grepl("CHILDHOOD",toupper(proj))) %>%
  select(rpt_date,col1,col2,col3,col4,col5) %>%
  left_join(pg_rg %>% select(rpt_date,rpt_ord),by="rpt_date")

childhood_lst<-list(gpc=childhood_abx %>% filter(col1=="GPC") %>%
                      rename(part_perc=col2,part_cnt=col3,
                             avg_day2active=col4),
                    site=childhood_abx %>% filter(col1!="GPC") %>%
                      rename(site=col1,
                             part_ind=col2,
                             day2active=col3,
                             success_query=col4,
                             perc_contr=col5))

#--front door general
front_door_general<-front_door_query %>%
  filter(token_index=="B.") %>%
  select(rpt_date,site,col2) %>%
  mutate(site=gsub(";;.*","",site)) %>%
  separate(col2,c("v1","v2","v3"),sep="(\\(|\\)|(of))",
           fill="left",remove=T) %>%
  gather(var,val,-rpt_date,-site) %>%
  mutate(val=trimws(val),
         val=case_when(grepl("\\.",val)~paste0(val,"%"),
                       TRUE ~ val),
         val2=as.integer(val)) %>%
  group_by(rpt_date,site) %>%
  mutate(val2_den=max(val2,na.rm=T)) %>%
  ungroup %>%
  mutate(var=case_when(grepl("%",val)~"percentage",
                       val2==val2_den ~ "cnt_den",
                       TRUE ~ "cnt_num")) %>%
  select(-val2,-val2_den) %>% unique %>% 
  spread(var,val) %>%
  mutate(percentage=as.numeric(gsub("\\%.*","",percentage)),
         cnt_num=case_when(percentage==100~cnt_den,
                           TRUE~cnt_num)) %>%
  left_join(pg_rg %>% select(rpt_date,rpt_ord),by="rpt_date")

#front door for covid
front_door_covid<-front_door_query %>%
  filter(token_index=="C."&grepl("(COVID)+",col1)) %>%
  select(rpt_date,site,col2) %>%
  mutate(site=gsub(";;.*","",site)) %>%
  unique %>%
  left_join(pg_rg %>% select(rpt_date,rpt_ord),by="rpt_date")

covid<-research_contr %>%
  filter(grepl("COVID",toupper(proj))) %>%
  select(rpt_date,proj,col1,col2,col3) %>%
  rename(site=col1,
         part_ind=col2) %>%
  filter(part_ind=="Yes") %>%
  separate(col3,c("cnt_num","cnt_den","percentage"),sep="(\\(|\\)|(of))",
           fill="left",remove=T) %>%
  mutate(cnt_num=as.numeric(cnt_num),
         cnt_den=as.numeric(cnt_den),
         percentage=as.numeric(gsub("\\%.*","",percentage))) %>%
  left_join(pg_rg %>% select(rpt_date,rpt_ord),by="rpt_date")


saveRDS(list(adaptable=adaptable_lst,
             preventable=preventable_lst,
             hero=hero_lst,
             bp_ctrl=bp_lst,
             bariatric=bariatric_lst,
             childhood_abx=childhood_lst,
             covid=list(covid=covid,
                        front_door_covid=front_door_covid),
             front_door_general=front_door_general),
        file="./data/Contr_Proj.rda")

#---------------------------------------------------------------
# Parse EDC Report
#---------------------------------------------------------------
path_to_folder<-file.path("C:/Users/xsm7f",
                          "University of Missouri",
                          "NextGen-BMI - Ogrp - Documents",
                          "GPC_ADMIN",
                          "GPC EDC Reports",
                          "2022-Q2")
file_lst<-list.files(path_to_folder)
file_lst<-file_lst[grepl("(EDCRPT)+.*(pdf)",file_lst)]

# pat_tbl<-c()
obspat_tbl<-c()
encpat_tbl<-c()
dxenc_tbl<-c()
pxenc_tbl<-c()
rxnorm_tbl<-c()
loinc_tbl<-c()
for(subpath in file_lst){
  file_i<-file.path(path_to_folder, subpath)
  file_parse_lst<-pdf_text(file_i) %>% strsplit("\n")
  
  #get page number
  get_pg_num<-function(doc,key){
    idx<-0
    pg<-1
    pg_end<-length(file_parse_lst)
    while(idx==0&pg<=pg_end){
      idx<-as.numeric(grepl(paste0("^(",key,")+"),doc[[pg]][[1]]))
      pg<-pg+1
    }
    return(pg-1)
  }
  
  #demographic distribution - Table IA
  # pn<-get_pg_num(file_parse_lst,"Table IA")
  # array_df<-tibble(rowid=seq_along(file_parse_lst[[pn]]),
  #                  key=file_parse_lst[[pn]]) %>% 
  #   slice(5:27) %>% #fixed table size in EDC report, by inspection
  #   mutate(key_ws=str_length(key)-str_length(trimws(key,"left")),
  #          key=trimws(key,"both")) %>%
  #   mutate(key_split=strsplit(key,"(\\s{2,})")) %>%
  #   unnest(key_split) %>%
  #   mutate(key_split=case_when(grepl("^(DEM_)+",key_split)~NA_character_,
  #                              TRUE ~ key_split)) %>%
  #   group_by(rowid) %>%
  #   mutate(col=paste0("col",1:n())) %>%
  #   ungroup %>% spread(col,key_split) %>%
  #   select(key_ws,col1,col2,col3) %>%
  #   rename(cat=col1,val=col2,perc=col3) %>%
  #   mutate(var=case_when(key_ws==2~cat)) %>%
  #   fill(var,.direction="down") %>%
  #   filter(!is.na(val)) %>%
  #   select(-key_ws)
    
  # pat_tbl %<>% 
  #   bind_rows(array_df %>%
  #               mutate(site=gsub("/.*","",subpath)))
  
  #informative cohort - Table IB 
  pn<-get_pg_num(file_parse_lst,"Table IB")
  array_df<-tibble(rowid=seq_along(file_parse_lst[[pn]]),
                   key=file_parse_lst[[pn]]) %>% 
    mutate(key_ws=str_length(key)-str_length(trimws(key,"left")),
           key=trimws(key,"both")) %>%
    filter(key_ws<=1&grepl("^(Patient|Potential)+",key)) %>% #number is always on the first line
    
    mutate(key_split=strsplit(key,"(\\s{2,})")) %>%
    unnest(key_split) %>%
    mutate(key_split=case_when(grepl("^(DEM_)+",key_split)~NA_character_,
                               TRUE ~ key_split)) %>%
    group_by(rowid) %>%
    mutate(col=paste0("col",1:n())) %>%
    ungroup %>% spread(col,key_split) %>%
    mutate(var=c("pat_obs","pat_trial","pat_pheno","pat_dx_perc","pat_px_perc"),
           val=case_when(grepl("^[0-9]",col3)~gsub(",","",col3),
                         TRUE~gsub(",","",col2))) %>%
    select(var,val)
  
  obspat_tbl %<>% 
    bind_rows(array_df %>%
                mutate(site=gsub("_.*","",subpath)))
  
  #encounter#/pat - Table IIID
  pn<-get_pg_num(file_parse_lst,"Table IIID")
  array_df<-tibble(rowid=seq_along(file_parse_lst[[pn]]),
                   key=file_parse_lst[[pn]]) %>%
    mutate(key = gsub("ENC_L3_ENCTYPE","",key)) %>%
    slice(c(14:18,20:25)) %>% #fixed table size in EDC report, by inspection
    separate("key",c("enc_type","enc","pat","enc_pat_ratio","enc_w_prov","vis_w_prov","proc_per_visit"),"\\s{4,}+",
             extra = "merge", fill = "right") %>%
    slice(-1) %>% select(-rowid) %>%
    mutate(across(enc:proc_per_visit, ~ as.numeric(gsub(",","",.x)))) %>%
    replace(is.na(.),0)
  
  encpat_tbl %<>% 
    bind_rows(array_df %>%
                mutate(site=gsub("_.*","",subpath)))
  
  #diagnosis#/enc - Table IVA
  pn<-get_pg_num(file_parse_lst,"Table IVA")
  array_df<-tibble(rowid=seq_along(file_parse_lst[[pn]]),
                   key=file_parse_lst[[pn]]) %>%
    mutate(key = gsub("DIA_L3_ENCTYPE;","",key)) %>%
    slice(c(10,13,16,19,22,25,28,31,34,37,40)) %>% #fixed table size in EDC report, by inspection
    separate("key",c("enc_type","dx_rec","dx_rec_enc_known","enc","dx_rec_per_enc","dx_rec_per_enc_known"),"\\s{2,}+",
             extra = "merge", fill = "right") %>%
    select(-rowid) %>%
    mutate(across(dx_rec:dx_rec_per_enc_known, ~ as.numeric(gsub(",","",.x)))) %>%
    replace(is.na(.),0)
  
  dxenc_tbl %<>% 
    bind_rows(array_df %>%
                mutate(site=gsub("_.*","",subpath)))
  
  #diagnosis#/enc - Table IVA
  pn<-get_pg_num(file_parse_lst,"Table IVA")
  array_df<-tibble(rowid=seq_along(file_parse_lst[[pn]]),
                   key=file_parse_lst[[pn]]) %>%
    mutate(key = gsub("DIA_L3_ENCTYPE;","",key)) %>%
    slice(c(10,13,16,19,22,25,28,31,34,37,40)) %>% #fixed table size in EDC report, by inspection
    separate("key",c("enc_type","dx_rec","dx_rec_enc_known","enc","dx_rec_per_enc","dx_rec_per_enc_known"),"\\s{2,}+",
             extra = "merge", fill = "right") %>%
    select(-rowid) %>%
    mutate(across(dx_rec:dx_rec_per_enc_known, ~ as.numeric(gsub(",","",.x)))) %>%
    replace(is.na(.),0)
  
  dxenc_tbl %<>% 
    bind_rows(array_df %>%
                mutate(site=gsub("_.*","",subpath)))
  
  #procedure#/enc Table IVB
  pn<-get_pg_num(file_parse_lst,"Table IVB")
  array_df<-tibble(rowid=seq_along(file_parse_lst[[pn]]),
                   key=file_parse_lst[[pn]]) %>%
    mutate(key = gsub("PRO_L3_ENCTYPE;","",key)) %>%
    slice(c(10,13,16,19,22,25,28,31,34,37,40)) %>% #fixed table size in EDC report, by inspection
    separate("key",c("enc_type","px_rec","px_rec_enc_known","enc","px_rec_per_enc","px_rec_per_enc_known"),"\\s{2,}+",
             extra = "merge", fill = "right") %>%
    select(-rowid) %>%
    mutate(across(px_rec:px_rec_per_enc_known, ~ as.numeric(gsub(",","",.x)))) %>%
    replace(is.na(.),0)
  
  pxenc_tbl %<>% 
    bind_rows(array_df %>%
                mutate(site=gsub("_.*","",subpath)))
  
  #medication - Table IVH
  pn<-get_pg_num(file_parse_lst,"Table IVH")
  array_df<-tibble(rowid=seq_along(file_parse_lst[[pn]]),
                   key=file_parse_lst[[pn]]) %>%
    mutate(key_ws=str_length(key)-str_length(trimws(key,"left")),
           key=trimws(key,"both")) %>%
    filter(key_ws>0&grepl("(RXNORM_CUI)+",key)) %>% #number is always on the first line
    mutate(key_split=strsplit(key,"(\\s{2,})")) %>%
    unnest(key_split) %>%
    filter(grepl("^[0-9]",key_split)&grepl("((Tier 1)|(Unknown))+",key)) %>%
    mutate(var = c(rep("tier1",2),rep("tier1_brand",2),rep("unknown",2),rep("tier1_5yr",2)),
           val_type = rep(c("cnt","perc"),4),
           val = as.numeric(gsub(",","",key_split))) %>%
    select(var,val,val_type) %>% mutate(tbl="prescribing") 
  
  if(!grepl("^((C4UMO|C4IHC|C4MCRF|C4WU))",subpath)){
    array_df %<>%
      bind_rows(tibble(rowid=seq_along(file_parse_lst[[pn+1]]),
                       key=file_parse_lst[[pn+1]]) %>%
                  mutate(key_ws=str_length(key)-str_length(trimws(key,"left")),
                         key=trimws(key,"both")) %>%
                  filter(key_ws>0&grepl("(MEDADMIN_CODE)+",key)) %>% #number is always on the first line
                  mutate(key_split=strsplit(key,"(\\s{2,})")) %>%
                  unnest(key_split) %>%
                  filter(grepl("^[0-9]",key_split)&grepl("((Tier 1)|(Unknown))+",key)) %>%
                  mutate(var = c(rep("tier1",2),rep("unknown",2),rep("tier1_5yr",2)),
                         val_type = rep(c("cnt","perc"),3),
                         val = as.numeric(gsub(",","",key_split))) %>%
                  select(var,val,val_type) %>% mutate(tbl="med_admin"))
  }
  
  rxnorm_tbl %<>% 
    bind_rows(array_df %>%
                mutate(site=gsub("_.*","",subpath)))
  
  #labs - Table IVI
  pn<-get_pg_num(file_parse_lst,"Table IVI")
  array_df<-tibble(rowid=seq_along(file_parse_lst[[pn]]),
                   key=file_parse_lst[[pn]]) %>%
    mutate(key_ws=str_length(key)-str_length(trimws(key,"left")),
           key=trimws(key,"both")) %>%
    filter(key_ws>0&grepl("(LAB_LOINC)+",key)) %>% #number is always on the first line
    mutate(key_split=strsplit(key,"(\\s{2,})")) %>%
    unnest(key_split) %>%
    filter(grepl("^[0-9]",key_split)&!grepl("^(3\\.)",key_split)) %>%
    mutate(var = c("loinc_cnt",rep("loinc_mapped",3),rep("loinc_mapped_quant",3)),
           val_type = c("cnt",rep(c("num","den","perc"),2)),
           val = as.numeric(gsub(",","",key_split))) %>%
    select(var,val,val_type)
  
  loinc_tbl %<>% 
    bind_rows(array_df %>%
                mutate(site=gsub("_.*","",subpath)))
  
  #labs - Table IVI
  pn<-get_pg_num(file_parse_lst,"Table IVI")
  array_df<-tibble(rowid=seq_along(file_parse_lst[[pn]]),
                   key=file_parse_lst[[pn]]) %>%
    mutate(key_ws=str_length(key)-str_length(trimws(key,"left")),
           key=trimws(key,"both")) %>%
    filter(key_ws>0&grepl("(LAB_LOINC)+",key)) %>% #number is always on the first line
    mutate(key_split=strsplit(key,"(\\s{2,})")) %>%
    unnest(key_split) %>%
    filter(grepl("^[0-9]",key_split)&!grepl("^(3\\.)",key_split)) %>%
    mutate(var = c("loinc_cnt",rep("loinc_mapped",3),rep("loinc_mapped_quant",3)),
           val_type = c("cnt",rep(c("num","den","perc"),2)),
           val = as.numeric(gsub(",","",key_split))) %>%
    select(var,val,val_type)
  
  loinc_tbl %<>% 
    bind_rows(array_df %>%
                mutate(site=gsub("_.*","",subpath)))
  
  #selective labs - Table IG
  # pn<-get_pg_num(file_parse_lst,"Table IG")
  # array_df<-tibble(rowid=seq_along(file_parse_lst[[pn]]),
  #                  key=file_parse_lst[[pn]])
  
}

# pat_tbl %<>%
#   mutate(val=as.numeric(gsub(",","",val)),
#          perc=as.numeric(gsub("\\%","",perc)))

obspat_tbl %<>%
  mutate(val_type=case_when(grepl("\\%",val)~"perc",
                            TRUE~"cnt")) %>%
  mutate(val=as.numeric(gsub(",","",gsub("\\%","",val))))
write.csv(obspat_tbl,file="C:/repo/GROUSE/res/observational-cohort.csv")

ggplot(encpat_tbl %<>%
         mutate(ratio_new = convert_scale(enc_pat_ratio,enc)[["val"]],
                axis_formula = convert_scale(enc_pat_ratio,enc)[["formula"]]),
       aes(x=site)) +
  geom_col(aes(y=enc), size = 1, color = "darkblue", fill = "white")+
  geom_line(aes(y=ratio_new), size = 1.5, color="red", group = 1)+
  scale_y_continuous(sec.axis = sec_axis(as.formula(encpat_tbl$axis_formula[1]), name = "enc_pat_ratio"))+
  theme(axis.text.x = element_text(angle = 45),text=element_text(face="bold")) +
  facet_wrap(~ enc_type,ncol=2,scales = "free")
ggsave(file="C:/repo/GROUSE/res/enc-per-pat.png",width=10,height=12)
write.csv(encpat_tbl,file="C:/repo/GROUSE/res/enc-per-pat.csv")

ggplot(dxenc_tbl %<>%
         mutate(new_col = convert_scale(dx_rec_per_enc,dx_rec)[["val"]],
                axis_formula = convert_scale(dx_rec_per_enc,dx_rec)[["formula"]]),
       aes(x=site)) +
  geom_col(aes(y=dx_rec), size = 1, color = "darkblue", fill = "white")+
  geom_line(aes(y=new_col), size = 1.5, color="red", group = 1)+
  scale_y_continuous(sec.axis = sec_axis(as.formula(dxenc_tbl$axis_formula[1]), name = "dx_enc_ratio"))+
  theme(axis.text.x = element_text(angle = 45),text=element_text(face="bold")) +
  facet_wrap(~ enc_type,ncol=2,scales = "free")
ggsave(file="C:/repo/GROUSE/res/dx-per-enc.png",width=10,height=12)
write.csv(dxenc_tbl,file="C:/repo/GROUSE/res/dx-per-enc.csv")


ggplot(pxenc_tbl %<>%
         mutate(new_col = convert_scale(px_rec_per_enc,px_rec)[["val"]],
                axis_formula = convert_scale(px_rec_per_enc,px_rec)[["formula"]]),
       aes(x=site)) +
  geom_col(aes(y=px_rec), size = 1, color = "darkblue", fill = "white")+
  geom_line(aes(y=new_col), size = 1.5, color="red", group = 1)+
  scale_y_continuous(sec.axis = sec_axis(as.formula(pxenc_tbl$axis_formula[1]), name = "px_enc_ratio"))+
  theme(axis.text.x = element_text(angle = 45),text=element_text(face="bold")) +
  facet_wrap(~ enc_type,ncol=2,scales = "free")
ggsave(file="C:/repo/GROUSE/res/px-per-enc.png",width=10,height=12)
write.csv(pxenc_tbl,file="C:/repo/GROUSE/res/px-per-enc.csv")

ggplot(rxnorm_tbl %<>% filter(val_type=="perc"&var!="unknown") %>%
         mutate(site_label=case_when(val <= 80 ~ site,
                                     TRUE ~ "")),
=======

ggplot(rxnorm_tbl %>% filter(val_type=="perc"&var!="unknown"),
>>>>>>> main:src/dstats/extract_rpt.R
       aes(x=var,y=val,fill=tbl)) +
  geom_boxplot(width=0.6) +
  stat_summary(geom="text", fun=quantile,
               aes(label=sprintf("%1.1f", ..y..)),
               position=position_nudge(x=0.33),
               size=4,fontface = "bold") +
<<<<<<< HEAD:src/dstats/R/extract_rpt.R
  geom_label_repel(aes(label=site_label),fill="grey",alpha=0.7)+
=======
>>>>>>> main:src/dstats/extract_rpt.R
  theme_bw() + 
  theme(text = element_text(face = "bold", size = 12),
        legend.position="none",
        axis.line = element_line(colour = "grey50"))+
  labs(x="RXNORM Tier", y="Mapping Percentage",title = "Figure 2. RXNORM_CUI mapping rates") +
  facet_wrap(~tbl,ncol=2)
<<<<<<< HEAD:src/dstats/R/extract_rpt.R
ggsave(file="C:/repo/GROUSE/res/rxcui_mapping_annotated.png",width=10,height=12)
write.csv(rxnorm_tbl,file="C:/repo/GROUSE/res/rxcui_mapping.csv")


ggplot(loinc_tbl %<>% filter(val_type=="perc") %>%
         mutate(site_label=case_when(val <= 80 ~ site,
                                     TRUE ~ "")),
=======
         
ggplot(loinc_tbl %>% filter(val_type=="perc"),
>>>>>>> main:src/dstats/extract_rpt.R
       aes(x=var,y=val,fill=var)) +
  geom_boxplot(width=0.6) +
  stat_summary(geom="text", fun=quantile,
               aes(label=sprintf("%1.1f", ..y..)),
               position=position_nudge(x=0.33),
               size=4,fontface = "bold") +
<<<<<<< HEAD:src/dstats/R/extract_rpt.R
  geom_label_repel(aes(label=site_label),fill="grey",alpha=0.7)+
=======
>>>>>>> main:src/dstats/extract_rpt.R
  theme_bw() + 
  theme(text = element_text(face = "bold", size = 12),
        legend.position="none",
        axis.line = element_line(colour = "grey50"))+
  labs(x="LOINC Mapping Type", y="Mapping Percentage",title = "Figure 1. LOINC mapping rates")
<<<<<<< HEAD:src/dstats/R/extract_rpt.R
ggsave(file="C:/repo/GROUSE/res/loinc_mapping_annotated.png",width=10,height=12)
write.csv(rxnorm_tbl,file="C:/repo/GROUSE/res/loinc_mapping.csv")
=======
>>>>>>> main:src/dstats/extract_rpt.R

#GPC counts aggregation
# gpc_cnt<-pat_tbl %>%
#   filter(!is.na(val)&!cat %in% c("Mean","Median")) %>%
#   group_by(var,cat) %>%
#   summarise(val=sum(val),.groups="drop") %>%
#   mutate(den=val[.$var=="Patients"]) %>%
#   mutate(perc=round(val/den*100,1),site="GPC")
# 
# obs_gpc<-obspat_tbl %>%
#   filter(val_type=="cnt") %>%
#   group_by(var,val_type) %>%
#   summarise(val=sum(val),.groups="drop") 
# 
# #GPC numeric summary estimation
# gpc_num<-pat_tbl %>%
#   filter(!is.na(val)&cat %in% c("Mean","Median")) %>%
#   left_join(pat_tbl %>% 
#               filter(var=="Patients") %>%
#               select(site,val) %>%
#               mutate(wt=val/gpc_cnt$den[1]) %>%
#               select(-val),
#             by="site") %>%
#   group_by(var,cat) %>%
#   summarise(val=round(sum(val*wt)),.groups="drop")
# 
# obs_gpc_num<-obspat_tbl %>%
#   filter(val_type=="perc") %>%
#   left_join(pat_tbl %>% 
#               filter(var=="Patients") %>%
#               select(site,val) %>%
#               mutate(wt=val/gpc_cnt$den[1]) %>%
#               select(-val),
#             by="site") %>%
#   group_by(var,val_type) %>%
#   summarise(val=round(sum(val*wt)),.groups="drop")
# 
# pat_tbl %<>% 
#   bind_rows(gpc_cnt %>% select(-den)) %>%
#   bind_rows(gpc_num %>% mutate(site="GPC"))
# 
# obspat_tbl %<>% 
#   bind_rows(obs_gpc %>% mutate(site="GPC")) %>%
#   bind_rows(obs_gpc_num %>% mutate(site="GPC"))
# 
# saveRDS(list(pat_tbl=pat_tbl,obspat_tbl=obspat_tbl),
#         file="./data/Demo_Overview.rda")


