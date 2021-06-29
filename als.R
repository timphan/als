#install packages needed for data wrangling
install.packages(c("dplyr", "readr"))

#read packages
library(dplyr)
library(readr)

#import the csv's directly by using the read_csv function from the readr package
info  <- read_csv(url("https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons.csv"))
emails<- read_csv(url("https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email.csv"))
status<- read_csv(url("https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email_chapter_subscription.csv"))

#we need to do some basic data wrangling before joining the datasets! we only care about subscription statuses with chapter_id = 1, so filter for that
status<- status %>% filter(chapter_id == 1) 

#To start our joins - the key between the "status" dataset and the "emails" is "cons_email_id", 
#I noticed that there were 2 versions of "modified_dt" in these datasets, one corresponds to when the status of the subscription was updated and one is when the user's information is updated. I renamed them accordingly to avoid confusion when joining.

emails <- emails %>% rename(modified_user_dt = modified_dt)
status <- status %>% rename(modified_status_dt = modified_dt)

#we are filtering for emails that are the primary emails, and sorting by "cons_id"
emails<- emails %>% filter(is_primary == 1) %>% 
  arrange(cons_id)

#Lastly, we join that dataset to the "info" dataset using "cons_id" as the key.
cons_id<- left_join(info, emails, by = "cons_id")
#Then we join that dataset to the "status" dataset using "cons_email_id" as the key.
cons_email_id<- left_join(cons_id, status, by = "cons_email_id")


#create the final "people" dataset by selecting only the 5 columns needed, rename them according, and format the date
people<- cons_email_id %>% select(email, source, isunsub, create_dt.x, modified_dt) %>%
  rename(code = source, is_unsub = isunsub, created_dt = create_dt.x, updated_dt = modified_dt) %>%
  mutate(created_dt = substring(created_dt, 6,15))

#create the final "aquisitions_facts" dataset by using a grouped_by aggregation function with dplyr
acquisition_facts<- people %>% group_by(created_dt) %>% 
  tally() %>% 
  rename(acquisition_date = created_dt, acquisitions = n)

#export both files using the "write.csv" function to your local directory
write.csv(people, "people.csv")
write.csv(acquisition_facts, "acquisition_facts.csv")


