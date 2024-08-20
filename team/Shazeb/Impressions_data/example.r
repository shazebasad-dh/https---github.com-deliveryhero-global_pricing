### Load and look at dataset
## Load tidyverse and mlogit
library(tidyverse)
library(mlogit)
## Load dataset from mlogit package
data('Heating', package = 'mlogit')
## Rename dataset to lowercase
heating <- Heating
## Look at dataset
tibble(heating)
## Pivot into a long dataset
heating_long <- heating %>% 
  pivot_longer(contains('.')) %>% 
  separate(name, c('name', 'alt')) %>% 
  pivot_wider() %>% 
  mutate(choice = (depvar == alt)) %>% 
  select(-depvar)
## Look at long dataset
tibble(heating_long)