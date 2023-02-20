rm(list=ls())
gc()

source("./R/util.R")

require_libraries(c("dplyr",
                    "tidyr",
                    "magrittr",
                    "ggplot2",
                    "maps",
                    "grid",
                    "gridExtra",
                    "ggthemes",
                    "ggrepel"))

dat<-read.csv("./data/grouse_overview.csv",
              stringsAsFactors = F) %>%
  mutate(pat_cnt_log10=pmax(0,round(log10(pat_cnt),1)),
         pat_cnt_K=pmax(0,round(pat_cnt/1000,1)),
         pat_cnt_10K=pmax(0,round(pat_cnt/10000,1)),
         pat_cnt_100K=pmax(0,round(pat_cnt/100000,1)),
         pat_cnt_200K=pmax(0,round(pat_cnt/200000,1)),
         pat_cnt_M=pmax(0,round(pat_cnt/1000000,1))) %>%
  mutate(pat_cnt_resize=pat_cnt_200K)

us<-map_data("state") %>% 
  inner_join(state.fips %>% select(abb,polyname),c("region"="polyname")) %>%
  left_join(dat %>% select(state,gpc_ind),by=c("abb"="state"))

# annote_site<-site<-read.csv("C:/Users/xsong/Documents/#Projects/GPC_Projects/GPC_Visual/data/mile_cov.csv",stringsAsFactors = F) %>%
#   left_join(read.csv("C:/Users/xsong/Documents/#Projects/GPC_Projects/GPC_Visual/data/gpc_overview.csv",stringsAsFactors = F),
#             by="state") %>%
#   mutate(state=tolower(state)) %>%
#   mutate(site_short_mk=paste0(site_short,
#                               case_when(updated==0~"*",
#                                         TRUE~"")),
#          site_full_mk=paste0(site_full,
#                              case_when(updated==0~"*",
#                                        TRUE~""))) %>%
#   select(state,site_lon,site_lat,site_short_mk,site_full_mk) %>%
#   inner_join(us %>% select(region,abb) %>% unique,by=c("state"="region"))

annote<-data.frame(
  long=state.center$x,
  lat=state.center$y,
  state_abb=state.abb,
  stringsAsFactors = F) %>%
  left_join(dat %>% filter(pop_type=="cms population") %>% 
              select(state,pat_cnt,pat_cnt_M,pat_cnt_resize,gpc_ind) %>%
              dplyr::rename("cms_pop"="pat_cnt_resize"),
            by=c("state_abb"="state")) %>%
  left_join(dat %>% filter(pop_type=="cross walk") %>% 
              select(state,pat_cnt,pat_cnt_K,pat_cnt_resize) %>%
              dplyr::rename("xw_pop"="pat_cnt_resize"),
            by=c("state_abb"="state")) %>%
  mutate(xw_perc=round(pat_cnt.y/pat_cnt.x*100)) %>%
  mutate(label=case_when(gpc_ind==1 ~ paste0(pat_cnt_M,"M(",pat_cnt_K,"K,",xw_perc,"%)"),
                         TRUE ~ ''))

scale_size_max<-max(annote$cms_pop)
scale_size_min<-min(annote$xw_pop)

us_cut<-us %>% filter(long>=-115&long<=-80)
annote_cut<-annote %>% filter(long>=-115&long<=-80)

####figure 1: GROUSE overview####
ggplot(us_cut,aes(x=long,y=lat))+
  geom_polygon(aes(fill=as.factor(gpc_ind),group=group),alpha=0.5,color='light grey')+
  geom_point(data=annote_cut,aes(x=long,y=lat,size=cms_pop,color="orange"),alpha=0.3) +
  geom_point(data=annote_cut,aes(x=long,y=lat,size=xw_pop,color="red"),alpha=0.5) +
  scale_size(range = c(scale_size_min,scale_size_max),guide="none")+
  theme(legend.position="none")+
  geom_label_repel(data=annote_cut,
                   aes(x=long,y=lat,label=state_abb),
                   label.size = NA, 
                   alpha = 0.2, 
                   size=3.5,
                   label.padding=.1, 
                   na.rm=TRUE,
                   seed = 1234)+
  geom_label_repel(data=annote_cut,
                   aes(label=state_abb),
                   label.size = NA,
                   alpha = 1, 
                   color = "dark grey",
                   size=3.5,
                   label.padding=.1, 
                   na.rm=TRUE,
                   fill = NA,
                   seed = 1234)+
  geom_text_repel(data=annote_cut, aes(label=label),
                  size=4,
                  fontface="bold")+
  labs(subtitle = "Figure 1 - CMS coverage and linkable patient distribution over GPC states",
       caption = "Data Source: GROUSE\n(% is out of total crosswalk population)")+
  scale_fill_discrete(guide="none")+
  scale_color_manual(breaks=c("orange","red"),
                     values=c("orange","red"),
                     labels=c("Overall CMS","Crosswalked"),
                     name="") +
  guides(color=guide_legend(override.aes = list(size=6)))+
  theme(text=element_text(face="bold",size=15),
        panel.background = element_rect(fill = NA, colour = NA),
        panel.grid.major = element_line(colour = NA),
        legend.position = c(0.2,0.2),
        legend.background = element_rect(),
        legend.text = element_text(size=10,face="bold"),
        legend.key.size = unit(0.5, 'lines'),
        legend.margin=margin(t = 0, unit='cm'))
  

#####figure 2: Overview of pre-approved GROUSE cohorts (obesity, ALS, Breast Cancer)####
dat_state<-dat %>% filter(gpc_ind==1) %>%
  dplyr::select(state,pop_type,pat_cnt) %>%
  spread(pop_type,pat_cnt,fill=0) %>%
  # mutate(obesity=round(obesity/`cross walk`,3),
  #        als=round(als/`cross walk`,3),
  #        `breast cancer`=round(`breast cancer`/`cross walk`,3)) %>%
  dplyr::select(state,`cross walk`,obese_adult,obese_child,als,`breast cancer`) %>%
  gather(pop_type,pat_cnt,-state,-`cross walk`) %>%
  mutate(prop=round(pat_cnt/`cross walk`,3),
         label=paste0(prop*100,"%"))

#draw the map canvas
map_cavas<-ggplot(us_cut,aes(x=long,y=lat))+
  geom_polygon(aes(fill=as.factor(gpc_ind),group=group),alpha=0.5,color='light grey')+
  geom_point(data=annote_cut,aes(x=long,y=lat,size=xw_pop),alpha=0.5,color="orange") +
  scale_size(range = c(min(annote_cut$xw_pop),max(annote_cut$xw_pop)),guide = 'none')+
  geom_label_repel(data=annote_cut,
                   aes(x=long,y=lat,label=state_abb),
                   label.size = NA, 
                   alpha = 0.2, 
                   size=3.5,
                   label.padding=.1, 
                   na.rm=TRUE,
                   seed = 1234)+
  geom_label_repel(data=annote_cut,
                   aes(label=state_abb),
                   label.size = NA,
                   alpha = 1, 
                   color = "dark grey",
                   size=3.5,
                   label.padding=.1, 
                   na.rm=TRUE,
                   fill = NA,
                   seed = 1234)+
  scale_fill_discrete(guide = 'none')+
  labs(subtitle = "Figure 2 - Pre-approved Cohort size (obesity, ALS, breast cancer) with linkage",
       caption = "Data Source: GROUSE\n(% is out of total crosswalk population)")+
  theme(text=element_text(face="bold",size=15),
        panel.background = element_rect(fill = NA, colour = NA),
        panel.grid.major = element_line(colour = NA))


#generate indivial barplot
state_lst<-unique(dat_state$state)

bar_panel<- 
  lapply(1:length(state_lst), function(i) { 
    dat_i<-dat_state[dat_state$state == state_lst[i],]
    gt_plot <- ggplotGrob(
      ggplot(dat_i)+
        geom_bar(aes(x=pop_type,y=log10(pat_cnt),fill=pop_type),
                 position='dodge',stat='identity',color="black") +
        # scale_y_continuous(sec.axis = sec_axis(~./dat_i$`cross walk`[1]))+
        geom_text(aes(x = pop_type,y=0.5,label = label),
                  position = position_dodge(0.9),size = 3.25, angle = 90,
                  color = "black", hjust = 'left',fontface="bold")+
        scale_y_continuous(limits=c(0,5.5))+
        labs(x = NULL, y = "log10(counts)") + 
        scale_fill_discrete(guide="none")+
        theme(rect = element_blank(),line = element_blank(),text = element_blank()) 
    )
    panel_coords <- gt_plot$layout[gt_plot$layout$name == "panel",]
    gt_plot[panel_coords$t:panel_coords$b, panel_coords$l:panel_coords$r]
  })

#add barplot to initial map
height<-5
width<-4
  
bar_annot<-
  lapply(1:length(state_lst), function(i) 
    annotation_custom(bar_panel[[i]], 
                      xmin = annote$long[annote$state_abb==state_lst[i]] - width/2,
                      xmax = annote$long[annote$state_abb==state_lst[i]] + width/2,
                      ymin = annote$lat[annote$state_abb==state_lst[i]],
                      ymax = annote$lat[annote$state_abb==state_lst[i]] + height))

result_plot <- Reduce(`+`, bar_annot, map_cavas)

result_plot +
  geom_point(data=data.frame(x=rep(-112,4),
                             y=rep(28,4),
                             color=letters[1:4]),
             aes(x=x,y=y,color=color))+
  scale_color_manual(breaks=letters[1:4],
                     values=gg_color_hue(4),
                     labels=c("ALS","Breast Cancer","Obesity(adult)","Obesity(pediatric)"),
                     name="") +
  guides(color=guide_legend(nrow=2,byrow=TRUE,
                            override.aes = list(size=6)))+
  theme(legend.position = c(0.2,0.2),
        legend.background = element_rect(),
        legend.text = element_text(size=10,face="bold"),
        legend.key.size = unit(0.5, 'lines'),
        legend.margin=margin(t = 0, unit='cm'))