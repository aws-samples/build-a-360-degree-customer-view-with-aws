## Build a 360-degree customer view in AWS using a powerful set of analytics tools

One of the challenges of digital transformation across companies is the task of consolidating customer data in one place, what we call a 360-degree customer view. This is a powerful concept in CRM (Customer Relationship Management) used on a daily basis on marketing, sales and customer services areas. Gather and aggregate information to build such view help us to have a complete vision of who are our customers, what are their preferences, what they have looked for, and how their preferences are correlated to other people preferences and behavior.

This implementation can be used to refine customer personas, their behavior and expectations, get to know touchpoints of interaction, perform a better post-sale service, cross-selling or up-selling, build personalized recommendation, map customer journey, and identify the gaps in channels or processes.

The common pain point is that information about customer is spread across several systems inside your company, from customer interaction with your call center records to events related to customer search behavior, you have several different data that are difficult to correlate if you don’t have a Data Lake strategy.

This git demonstrated how to bring data from different data systems as a set of customer dimensions and build a 360-degree customer view as a baseline for all customer analytics initiatives.

## Dimensions for a 360-degree customer view

Usually we look at our customers from a perspective, such as Loyalty perspective, looking at metrics like: years of history, frequency of interaction, or a Demographic perspective: stage of life, income, stage of life. We call them dimensions, and we will combine several dimensions about your customer to give you more visibility of several perspectives at the same time.

In this git we will explore a hypothetic financial services company, as there are common dimensions for this industry and some dimensions that are also valid for any service industry, like marketing and communications, customer history or demographic dimension.

![360-degree Customer View](images/pic1.png)


In the picture above we have list some of the dimensions, and the challenges we have to aggregate and use all this information is the number of different sources and formats.

For financial services we usually have a CRM (Customer Relationship Management) as a software as a service, that can be consumed by an API (Application Programming Interface) and provides JSON (JavaScript Object Notation) file format, Mainframe systems and some other systems that can be integrated by CSV (Comma-separated value) files, transactions from relational database generating thousands new records per minute and application logs or website navigation logs. New sources can come over time, like a new system or channel that are provided to customers.

## Data Lake strategy

Data lake is the source of truth, as a broad repository where we can put data from different systems and perform the cleaning, enrichment, aggregation, analysis to delivery relevant data to business users, about user behavior, assets, history, preferences and several other features.

To deliver summarized and at the same time comprehensive data about customer to business users and data scientists we need ingest data from the sources into a storage area and combine them to build a 360-degree customer view.

![Data Sources and Layers](images/pic2.png)








## License

This library is licensed under the MIT-0 License. See the LICENSE file.
