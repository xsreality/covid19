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

![Kafka Streams Architecture](https://i.ibb.co/d4Ld2TW/Covid19-India-Alerts-1.png "Covid19 Kafka Streams Architecture")