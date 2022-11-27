rm(list=ls()); gc()
setwd("C:/repo/GROUSE")

pacman::p_load(
  tidyverse,
  magrittr,
  readxl,
  httr,
  maps,
  gridExtra,
  grid,
  ggthemes,
  scales,
  sp
)

#=======summarize patient count at county level
path_to_folder<-file.path("C:/Users/xsm7f",
                          "University of Missouri",
                          "NextGen-BMI - Ogrp - Documents",
                          "GPC_ADMIN",
                          "GPC EDC Reports",
                          "Zipcount")
file_lst<-list.files(path = path_to_folder)
pat_cnt_zip<-c()
for(i in seq_along(file_lst)){
  file_i<-file.path(path_to_folder, file_lst[i])
  dat_i<-read.csv(file_i)
  colnames(dat_i)<-c("zip","pat_cnt")
  dat_i %<>% mutate(zip=str_pad(zip,5,"left","0"),
                    site=gsub(".*_","",strsplit(file_lst[i],"\\.")[[1]][1]))
  pat_cnt_zip %<>% bind_rows(dat_i)
}

# process masked counts --- reduce to 1
pat_cnt_zip %<>%
  mutate(pat_cnt_ud = case_when(pat_cnt <= 10 ~ 1),
         pat_cnt = coalesce(pat_cnt_ud,pat_cnt)) %>%
  select(-pat_cnt_ud)
  # mutate(zip=paste0("'",zip))
# process masked counts --- remove
# pat_cnt_zip %<>% filter(pat_cnt > 10)

# write.csv(pat_cnt_zip,file="C:/Users/xsm7f/University of Missouri/NextGen-BMI - Ogrp - Documents/GROUSE/gpc_patcnt_zip_v2.csv",
#           row.names = FALSE)

# get population at county level
pop_county<-read.csv("https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/totals/co-est2020-alldata.csv",
                     fileEncoding="latin1",header=T,stringsAsFactors = F) %>%
  select(STATE,COUNTY,STNAME,CTYNAME,CENSUS2010POP,POPESTIMATE2020) %>%
  mutate(geoid=paste0(str_pad(STATE,2,"left","0"),
                      str_pad(COUNTY,3,"left","0")),
         state=tolower(STNAME),
         county=str_replace(tolower(CTYNAME)," county","")) %>%
  mutate(county=str_replace_all(county,"((\\. )|( )|('))","")) %>% # county naming convention (remove dot,space,apostrophe)
  mutate(county=str_replace_all(county,"(parish)","")) %>% # county naming curation(remove "parish")
  mutate(county=str_replace(county,"(suffolkcity)","suffolk")) %>% # county naming curation("suffolkcity" to "suffolk")
  mutate(county=str_replace(county,"(hamptoncity)","hampton")) %>% # county naming curation("hamptoncity" to "hampton")
  mutate(county=str_replace(county,"(newportnewscity)","newportnews")) %>% # county naming curation("newportnewscity" to "newportnews")
  mutate(county=str_replace(county,"(norfolkcity)","norfolk")) %>% # county naming curation("norfolkcity" to "norfolk")
  mutate(county=str_replace(county,"(virginiabeachcity)","virginiabeach")) %>% # county naming curation("virginiabeachcity" to "virginiabeach")
  mutate(county=str_replace(county,"(doñaana)","donaana")) %>% # county naming curation("doñaana" to "donaana")
  mutate(county=str_replace(county,"(districtofcolumbia)","washington")) %>% # county naming curation("districtofcolumbia" to "washington")
  rename(county_pop10=CENSUS2010POP,
         county_pop20e=POPESTIMATE2020) %>%
  select(geoid,state,county,county_pop10,county_pop20e) %>%
  dplyr::mutate(area_type=ifelse(county_pop20e >= 50000,1,
                                 ifelse(county_pop20e < 50000 & county_pop20e >= 2500,2,
                                        3)))

# get zip to county crosswalk
GET("https://www.huduser.gov/portal/datasets/usps/ZIP_COUNTY_122021.xlsx", 
    write_disk(zip_cnty <- tempfile(fileext = ".xlsx")))
zip_county<-read_xlsx(zip_cnty) %>%
  select(zip,county) %>% unique %>%
  rename(geoid=county) %>%
  inner_join(pop_county %>% select(geoid,county,state,county_pop20e),
             by="geoid")

# zip to zcta crosswalk
# GET("https://udsmapper.org/wp-content/uploads/2021/09/ZiptoZcta_Crosswalk_2021.xlsx", 
#     write_disk(zip_zcta <- tempfile(fileext = ".xlsx")))
# zip_zcta <- read_excel(zip_zcta)


# FIPS codes DD
GET("https://www2.census.gov/programs-surveys/popest/geographies/2017/all-geocodes-v2017.xlsx",
    write_disk(fips <- tempfile(fileext = ".xlsx")))
fips <- read_excel(fips, skip=4)

pat_cnt_cnty<-pat_cnt_zip %>%
  inner_join(zip_county,by="zip") %>%
  group_by(geoid,county,state,county_pop20e) %>%
  summarise(pat_cnt = sum(pat_cnt),.groups="drop") %>%
  mutate(cnty_penatration=pmin(1,round(pat_cnt/county_pop20e,4))) %>%
  arrange(state,desc(cnty_penatration))

# saveRDS(pat_cnt_cnty,file="./data/gpc_cnty_summary.rda")

# get geographical markers of county and state
us_county<-map_data("county")
us_state<-map_data("state")

# join with data
gpc_pat_rate_longlat<-us_county %>% 
  mutate(subregion=str_replace_all(subregion," ","")) %>% #e.g. "de kalb" instead of "dekalb"
  mutate(subregion=ifelse(region=="south dakota" & subregion=="shannon",
                          "oglalalakota",subregion)) %>% # name change since 2015
  rename(state=region,county=subregion) %>%
  left_join(pat_cnt_cnty,by=c("state","county")) %>%
  replace_na(list(pat_cnt=0,county_pop20e=0,cnty_penatration=0)) %>%
  mutate(cnty_penatration_perc=round(cnty_penatration*100)) %>%
  mutate(pat_cnt_log10=ifelse(pat_cnt==0,0,log10(pat_cnt))) %>%
  arrange(desc(cnty_penatration))

# annotate state 
st_annotate<-data.frame(
  long=state.center$x,
  lat=state.center$y,
  state_abb=state.abb)

# annotate county
cnty_annotate<-us_county %>%
  group_by(group,region,subregion) %>%
  summarise(long=mean(range(long)),lat=mean(range(lat)),.groups="drop")

#thematic map1 -- overall patient counts
# ggplot()+
#   geom_polygon(data=gpc_pat_rate_longlat,
#                aes(x=long,y=lat,group=group,
#                    fill=patient_cnt_log10))+
#   geom_path(data = us_state,
#             aes(x=long,y=lat,group=group),
#             colour = "black", size = .3)+
#   geom_path(data = gpc_pat_rate_longlat,
#             aes(x=long,y=lat,group=group,colour = as.factor(area_type)),
#             size = .6, alpha = .8) +
#   # scale_fill_continuous("patient rate",trans = "reverse")+
#   scale_fill_gradientn("patient counts",
#                        colours = topo.colors(11),
#                        trans = "reverse",
#                        breaks=seq(0,6,by=1),
#                        labels=c("<10",paste0("10^",seq(1,5,by=1)),">10^6"))+
#   scale_color_discrete("Area Type",breaks=c(3,2,1),
#                        labels=c("Rural Areas (<2,500)","Urban Clusters (2,500-50,000)","Urbanized Areas (>=50,000)"))+
#   geom_text(data=stannote,aes(x=long,y=lat,label=state_abb),size=3)+
#   labs(x="longitude", y="latitude",
#        title="Patient Counts by County")+
#   theme_classic()


#thematic map2 -- penatration rate
# ggplot()+
#   geom_polygon(data=gpc_pat_rate_longlat,
#                aes(x=long,y=lat,group=group,
#                    fill=cnty_penatration))+
#   geom_path(data = us_state,
#             aes(x=long,y=lat,group=group),
#             colour = "black", size = .3)+
#   geom_path(data = gpc_pat_rate_longlat,
#             aes(x=long,y=lat,group=group,colour = as.factor(area_type)),
#             size = 0.6, alpha = 0.8) +
#   # scale_fill_continuous("patient rate",trans = "reverse")+
#   scale_fill_gradientn("patient rate(%)",
#                        colours = topo.colors(11),
#                        trans = "reverse",
#                        breaks=seq(0,100,by=10),
#                        labels=seq(0,100,by=10))+
#   scale_color_discrete("Area Type",breaks=c(3,2,1),
#                        labels=c("Rural Areas (<2,500)","Urban Clusters (2,500-50,000)","Urbanized Areas (>=50,000)"))+
#   geom_text(data=stannote,aes(x=long,y=lat,label=state_abb),size=3)+
#   labs(x="longitude", y="latitude",
#        title="Patient Penatration Rate by County",
#        subtitle="=(patient counts/estimated population at 2017)*100%",
#        caption="Note: rates above 100% are rounded down to 100%")+
#   theme_classic()



#thematic map3 -- penatration rate - target states
st_tgt<-c("arizona")
ggplot()+
  geom_polygon(data=gpc_pat_rate_longlat %>% filter(state %in% st_tgt),
               aes(x=long,y=lat,group=group,
                   fill=cnty_penatration_perc))+
  # state contour
  geom_path(data = us_state %>% filter(region %in% st_tgt),
            aes(x=long,y=lat,group=group),colour = "red", size = 1)+
  # county contour
  geom_path(data = us_county %>% filter(region %in% st_tgt),
            aes(x=long,y=lat,group=group),colour = "black", size = .3)+
  # county annotation
  geom_text(data=cnty_annotate %>% filter(region %in% st_tgt),
            aes(x=long,y=lat,label=subregion),size = 3) +
  # scale_fill_continuous("patient rate",trans = "reverse")+
  scale_fill_gradientn("patient rate(%)",
                       colours = topo.colors(11),
                       trans = "reverse",
                       breaks=seq(0,100,by=10),
                       labels=seq(0,100,by=10))+
  labs(x="longitude", y="latitude",
       title=sprintf("Patient Penatration Rate by County: %s",paste(st_tgt,collapse=",")),
       subtitle="=(patient counts/estimated population at 2020)*100%",
       caption="Note: rates above 100% (due to cross-site duplicates) are rounded down to 100%")+
  theme_classic()



## Illinois
cms_pop<-2265857
pop<-12720000
write.csv(gpc_pat_rate_longlat %>% filter(state %in% c("illinois")) %>% 
            select(geoid,state,county,cnty_penatration,county_pop20e) %>% unique %>%
            mutate(county_medicare_pop_est=ceiling(cms_pop/pop*county_pop20e)),
          file="./data/IL_county_map.csv",row.names = F)

## Wyoming
cms_pop<-113802
pop<-581348
write.csv(gpc_pat_rate_longlat %>% filter(state %in% c("wyoming")) %>% 
            select(geoid,state,county,cnty_penatration,county_pop20e) %>% unique %>%
            mutate(county_medicare_pop_est=ceiling(cms_pop/pop*county_pop20e)),
          file="./data/WY_county_map.csv",row.names = F)


## Michigan
cms_pop<-2100420
pop<-9974000
write.csv(gpc_pat_rate_longlat %>% filter(state %in% c("michigan")) %>% 
            select(geoid,state,county,cnty_penatration,county_pop20e) %>% unique %>%
            mutate(county_medicare_pop_est=ceiling(cms_pop/pop*county_pop20e)),
          file="./data/MI_county_map.csv",row.names = F)


## Nevada
cms_pop<-549393
pop<-3030000
write.csv(gpc_pat_rate_longlat %>% filter(state %in% c("nevada")) %>% 
            select(geoid,state,county,cnty_penatration,county_pop20e) %>% unique %>%
            mutate(county_medicare_pop_est=ceiling(cms_pop/pop*county_pop20e)),
          file="./data/NV_county_map.csv",row.names = F)

## Arizona
cms_pop<-142635
pop<-7174000
write.csv(gpc_pat_rate_longlat %>% filter(state %in% c("arizona")) %>% 
            select(geoid,state,county,cnty_penatration,county_pop20e) %>% unique %>%
            mutate(county_medicare_pop_est=ceiling(cms_pop/pop*county_pop20e)),
          file="./data/AR_county_map.csv",row.names = F)

#thematic map4 -- penatration rate - target states stratified by sites




