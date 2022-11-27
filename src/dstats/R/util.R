##---------------------------helper functions--------------------------------------##
## install (if needed) and require packages
require_libraries<-function(package_list,verb=T){
  for (lib in package_list) {
    chk_install<-!(lib %in% installed.packages()[,"Package"])
    if(chk_install){
      install.packages(lib)
    }
    library(lib, character.only=TRUE,lib.loc=.libPaths())
    if(verb){
      cat("\n", lib, " loaded.", sep="") 
    }
  }
}

#rule-based parsing
parse_pdf_pg<-function(array_to_parse,token,ncol_est=2,token_drop=1){
  #existence check
  if(length(array_to_parse)<1){
    stop("document to be parsed is not valid (e.g. empty document)!")
  }
  
  #match on each token
  #initialization
  array_df<-tibble(rowid=seq_along(array_to_parse),
                   key=array_to_parse) %>% 
    mutate(key_ws=str_length(key)-str_length(trimws(key,"left")),
           key=trimws(key,"both"),
           token_id=NA_character_,
           token_index=NA_character_
           )
  
  #token identification
  for(t in seq_along(token)){
    array_df %<>%
      #re-scan and update token_id column
      mutate(token_id=case_when(!is.na(token_id) ~ token_id,
                                grepl(token[t],key) ~ paste0("token_",t)),
             token_index=case_when(!is.na(token_index) ~ token_index,
                                   grepl(token[t],key) ~ str_extract(key,token[t])))
  }
  
  #clean-up
  array_df %<>%
    mutate(key_split=strsplit(key,"(\\s{2,})")) %>%
    rowwise() %>% mutate(split_len=length(unlist(key_split))) %>%
    ungroup

  #estimate ncol of embedded table
  col_n<-min(ncol_est,max(array_df$split_len))
  
  if(col_n < 3){
    array_df %<>%
      #splitting lines can be identified with table of less than 2 columns
      fill(token_id,.direction = "down") %>%
      fill(token_index,.direction = "down") %>%
      unnest(key_split) %>% 
      mutate(key_split=trimws(key_split,"both")) %>%
      group_by(token_id,token_index) %>%
      mutate(rowid=dense_rank(min(rowid,na.rm=T)),
             col1=paste(key_split[(substr(key,1,3)==substr(key_split,1,3)&key_ws<=10)],collapse = ";;"),
             col2=paste(key_split[!(substr(key,1,3)==substr(key_split,1,3)&key_ws<=10)],collapse = ";;")) %>%
      ungroup %>%
      select(-key_split,-key,-key_ws,-split_len) %>% unique
  }else{
    #for simplicity, further reduce to single-row lines
    array_df %<>% 
      filter(!is.na(token_id)) %>%
      unnest(key_split) %>%
      mutate(rowid=dense_rank(rowid)) %>%
      group_by(token_id,token_index) %>%
      mutate(colnm=paste0("col",1:n())) %>%
      ungroup %>%
      spread(colnm,key_split) %>% arrange(rowid) %>%
      select(-key,-key_ws,-split_len) %>% unique
  }
  
  #drop trivial rows
  if(length(token_drop)>0){
    array_df %<>%
      filter(!token_id %in% paste0("token_",token_drop))
  }
  
  return(array_df)
}

parse_proci_rpt<-function(array_to_parse,ncol_est=2,
                          template=c("NPR_2CollMultiLine",
                                     "NPR_MultiColMultiLine",
                                     "EDC")){
  if(template=="NPR_2CollMultiLine"){
    token<-c("^([0-9]\\.0)+",
             "^([0-9]\\.[1-9])+",
             paste0("((",paste(c("Allina",
                                 "IU",
                                 "Intermountain",
                                 "Marshfield",
                                 "MCW",
                                 "Iowa",
                                 "KUMC",
                                 "Missouri",
                                 "UNMC",
                                 "UTHSCSA",
                                 "UTSWMC",
                                 "Utah"),
                               collapse=")|("),"))+"),
             "^([A-Z]\\.)+",
             "^([0-9]{1,2}\\))+",
             "^([a-z]\\))+",
             "^(Date Generated)+"
             )
    
    parse_pdf_pg(array_to_parse,
                 token)
  }
  else if(template=="NPR_MultiColMultiLine"){
    token<-c("^([0-9]\\.0)+",
             "^([0-9]\\.[1-9])+",
             paste0("((",paste(c("(Network Status)",
                                 "(Site Status)",
                                 "(Study\\:)",
                                 "(Lead Site\\:)"),
                               collapse=")|("),"))+"),
             paste0("((",paste(c("GPC",
                                 "Allina",
                                 "IU",
                                 "Intermountain",
                                 "Marshfield",
                                 "MCW",
                                 "Iowa",
                                 "KUMC",
                                 "Missouri",
                                 "UNMC",
                                 "UTHSCSA",
                                 "UTSWMC",
                                 "Utah"),
                               collapse=")|("),"))+"),
             "^(Date Generated)+"
    )
    
    parse_pdf_pg(array_to_parse,
                 token,ncol_est=10)
  }
  else{
    stop("report template not available!")
  }
}
