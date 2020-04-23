# Covid19 India Patients Analyzer and Telegram Bot

This project analyzes the patients data obtained from the APIs built by Covid19India (https://api.covid19india.org/)
using Kafka topics and generates alerts. These alerts are picked up by Telegram Bot app which sends 
these alerts to anyone subscribed.

The Telegram Bot is available at https://t.me/covid19_india_alerts_bot

To subscribe, just send the command `/start`. To unsubscribe, send the command `/stop`.

## New Features

1. All updates carry news sources (links to State Govt / ANI tweets).
2. Updates include `Doubling rate` based on previous day statistics.

Below is an example of an alert sent by the bot:

![Sample Telegram Bot Alert screenshot](https://i.ibb.co/V3hQLwV/Screenshot-20200402-000351.jpg "Sample Telegram alert")
![Sample Telegram Bot Statewise Alert screenshot](https://i.ibb.co/hRw0Qt1/Screenshot-20200423-190044.jpg "Statewise alert")

## Alert Updates

The folks at Covid19India (https://www.covid19india.org/) are doing an amazing job of continuously
updating the patient information using official and unofficial (social media etc.) sources. The bot
tries to capture these updates and sends alerts that links to previous alert sent for the same patient (if any).
This helps you see the progress a patient makes (like recovered or deceased). Alerts are only sent
if the update is significant. Currently this means either changes in status or notes.

## Architecture

### Individual Patient Updates

![Kafka Streams Individual Patient Updates](https://i.ibb.co/MPZJjB3/Covid19-India-Alerts-3.png "Kafka Streams Individual Patient Updates")

### Cumulative + Delta + Daily Increase Updates

![Kafka Streams Architecture](https://i.ibb.co/Zd0Ng7F/Covid19-India-Alerts-4.png "Covid19 Kafka Streams Architecture")

## Telegram Commands

#### `/mystate` - Set preferred state

Not interested in updates of all Indian states? Then you can use the command `/mystate` to set your
_preferred_ state. Once set, you will receive updates of that state only including cumulative totals
across India. You will continue to receive summary updates every 4 hours.

#### `/clearmystate` - Remove preferred state

If you have set preferred state and would like to start receiving updates for _every_ state, then
send the command `/clearmystate` to clear your preferred state.

#### `/getmystate` - Get currently preferred state

At any time, you want to check what preferred state is set if any, then send the command `/getmystate`.

#### `/start` - Subscribe to automatic real-time updates

Get real-time updates of all Indian States

#### `/stop` - Unsubscribe from updates

Stop receiving updates.

#### `/stats` - On-demand Statistics

#### `/summary` - Get summary of all Indian States

#### `/total` - Get total count across India

To get the current statistics of any Indian State or Total, send the command `/stats`.

The bot will reply with an option to choose a region:

![Telegram command /stats choose region](https://i.ibb.co/B3Jmsn8/Screenshot-20200420-191540.jpg "Telegram command /stats choose region")

You can select any region or choose `Total` to get cumulative sum of all Indian states. If you choose
a region, next screen will pop-up asking to choose a state within the region.

![Telegram command /stats choose state](https://i.ibb.co/1dJxtZq/Screenshot-20200420-191611.jpg "Telegram command /stats choose state")

You should get back the cumulative statistics of the chosen state at that point of time.

![Telegram command /stats state summary](https://i.ibb.co/q1KdWc2/Screenshot-20200420-191310-01.jpg "Telegram command /stats state summary")

### Python Importer

The Python script that imports the Covid19India API is available here: https://github.com/xsreality/covid19-patient-importer

## Contributing

* Contributions for new ideas and open issues are welcome!
* Please open a GitHub issue before opening a pull request.
* For any queries, contact me on Telegram at https://t.me/xsreality  
