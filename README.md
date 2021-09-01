1. First, create copies of the following files:

  https://datastudio.google.com/s/t3im_Rlj6is
 
  https://docs.google.com/spreadsheets/d/1jAuoEFmslQRUijrPA3VLcVLEjCXXByQzlpueq-FTqHs/edit?usp=sharing
  
2. Add the list of domains (to the input tab) you want to monitor to the Google Sheet. Give it a friendly name under column A, the domain (in any format) in column B, and any known issues in column C

3. Open the py file and replace the values of all references to Google Sheets or DataStudio

4. Once you've set up Google CSE and the Google Sheets API, replace the key and cx values with your own

5. All variables that you need to set are denoted by a ### comment

6. The last piece is automation, I recommend using Task Scheduler if you're on Windows. I run this once a week but frequency is easily adjustable in TS.
