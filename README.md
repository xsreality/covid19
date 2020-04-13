# Covid19 India Patients Analyzer and Telegram Bot

This project analyzes the patients data obtained from the APIs built by Covid19India (https://api.covid19india.org/)
using Kafka topics and generates alerts. These alerts are picked up by Telegram Bot app which sends 
these alerts to anyone subscribed.

The Telegram Bot is available at https://t.me/covid19_india_alerts_bot

To subscribe, just send the command `/start`. To unsubscribe, send the command `/stop`.

Below is an example of an alert sent by the bot:

![Sample Telegram Bot Alert screenshot](https://i.ibb.co/V3hQLwV/Screenshot-20200402-000351.jpg "Sample Telegram alert")
![Sample Telegram Bot Statewise Alert screenshot](https://i.ibb.co/RyGHc1t/Screenshot-20200410-115109.jpg "Statewise alert")

## Alert Updates

The folks at Covid19India (https://www.covid19india.org/) are doing an amazing job of continuously
updating the patient information using official and unofficial (social media etc.) sources. The bot
tries to capture these updates and sends alerts that links to previous alert sent for the same patient (if any).
This helps you see the progress a patient makes (like recovered or deceased). Alerts are only sent
if the update is significant. Currently this means either changes in status or notes.

## Architecture

![Kafka Streams Architecture](https://i.ibb.co/r2zJFL2/Covid19-India-Alerts-2.png "Covid19 Kafka Streams Architecture")

## On-demand Statistics - Telegram command `/stats`

To get the current statistics of any Indian State or Total, send the command `/stats`.

The bot will reply with an option to choose a region:

![Telegram command /stats choose region](https://i.ibb.co/MPv3jk5/Screenshot-20200413-184036.jpg "Telegram command /stats choose region")

You can select any region or choose `Total` to get cumulative sum of all Indian states. If you choose
a region, next screen will pop-up asking to choose a state within the region.

![Telegram command /stats choose state](https://i.ibb.co/gPX3tjw/Screenshot-20200413-184138.jpg "Telegram command /stats choose state")

You should get back the cumulative statistics of the chosen state at that point of time.

![Telegram command /stats state summary](https://i.ibb.co/Y2m1Rhk/Screenshot-20200413-184246.jpg "Telegram command /stats state summary")
