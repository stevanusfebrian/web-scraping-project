# About
This repo is from requested project to scrape data from:
1. Dapodik 
2. VervalYayasan

# Projects:
1. dapodik-rekapitulasi: getting school recap of all available semester (status: done)
2. dapodik-school-profiles: getting school profile data (status: not done, progress (31 partitions scraped out of 100 partitions))
3. verval-yayasan: getting yayasan profile data, sekolah naungan, cabang yayasan (status: done)

# Result Dataset
the result dataset is store seperated because this code uploaded to github and the size of the result of each project cannot fit in to github repo

to access the result dataset, you can access it through [here](https://binusianorg-my.sharepoint.com/personal/stevanus_febrian_binus_ac_id/_layouts/15/guestaccess.aspx?share=EnnvxROUz8BGiKwTSEYeOg4Biq2wjqnPYcCnvjGE6Hh1rw&e=tKwUoa). it's the same link as in the `result-dataset`/`data-result` folder in each project

# Code Through Documentation:
here i help to explaining each project in this [video link](https://binusianorg-my.sharepoint.com/personal/stevanus_febrian_binus_edu/_layouts/15/guestaccess.aspx?share=Eo4EYzYRvY1HlTRR91Se3RIBrY-TomHaJugRPi1krj16wQ&e=6b3GTv):
1. purpose of the project
2. what data to get 
3. setup
4. explaining what the code does

# dapodik-school-profiles
to continue the scrape, you need to change the range of `for loop` red block shown below:
- currently it already scrape the 31st partition, so it needs to continue the 32-100 partition
- don't forget to rename the file in the orange block below
![code](https://github.com/stevanusfebrian/web-scraping-project/assets/100825866/29784dcf-663f-4a12-ad54-d6121776fd46)


Thanks.
