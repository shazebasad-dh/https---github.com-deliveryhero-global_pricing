## What is this for?

As of 2023Q2, Hungerstation (HS) data is not joineable with our tables due to BigQuery restriction. HS is stored in Europe, while Pricing is in USA. BigQuery doesn't allow to join data that are not stored in the same place.

Given this situation, we must first download the data to a local machine and then upload it back into our dataset dh-logistics-product-ops.pricing to have it available for analysis. 

The code in this repo does that. It takes X days of HS data, starting always from the current date, makes a temporary file with it and loads it back to BigQuery but to our table in the Pricing dataset. 

## How to use it

The easiest way is to open the [Colab Notebook](https://colab.research.google.com/drive/1PYBezUjsjqVijDOwJwjlemiLsRTAFWJT#scrollTo=zAEBRDf7FYK_) and run follows the instruction there. 

If you want to run it locally, you'd need to install the required libraries in the requirements.txt and also
have available a credentials.json file to authenticate yourself against Google. Assuming you've met the pre-requisities, replace the credentials path to your own in the dag.py file.  

To run the script, set your working directory to this folder, place yourself in the terminal, and run

```
python dag.py -d X
```

where X is the number of days you'd like to fetch data. For example, the following will fetch the last 14 days of data:

```
python dag.py -d 14
```

Questions/Comments/Problems, reach out to [@Sebastian Lafaurie](mailto:sebastian.lafaurie@deliveryhero.com)
