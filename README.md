## Luxury Used Car Trends: A Data-Driven Toronto Dealership Strategy

## Content

- [Background & Problem Statement](#background--problem-statement)
- [Objective](#objective)
- [Method](#method)
- [Execution Steps](#execution-steps)
- [Main Findings](#main-findings)
- [Conclusion](#conclusion)


## Background & Problem Statement

This analytical portfolio project centers on a simulated car dealership operating in downtown Toronto’s financial district—an area populated by young professionals in high-paced corporate environments. Like many actual dealerships, it faced significant headwinds during the Covid-19 pandemic, including a sharp rise in used car prices and constrained inventory due to global chip shortages. Between 2020 and 2022, customer interest in purchasing used vehicles declined notably.

In response, the dealership refined its branding strategy to focus on selling luxury used cars to young professionals and is now seeking actionable, data-driven insights to guide key operational decisions.

## Objective
This report leverages a publicly available dataset of used car advertisements to identify key trends and ultimately recommend 15 luxury car models with strong sales potential.

## Method
**Tools:** Google Cloud Platform (GCP), HDFS, Hive

**Language:** HiveQL

**Dataset:**
- Source: [kaggle (Classified Ads for Cars: Used cars for sale 2015 - 2017)](https://www.kaggle.com/datasets/mirosval/personal-cars-classifieds)
- Original Size: 3,552,912 rows across 16 variables
- Variables: (1) maker, (2) model, (3) mileage, (4) manufacture year, (5) engine displacement (in ccm), (6) engine power (in kW), (7) body type, (8) color slug, (9) year of the last emission control, (10) transmission (automatic/ manual), (11) door count, (12) seat count, (13) fuel type (gasoline/diesel/cng/lpg/electric), (14) date the ad was scraped, (15) the ad was last seen, (16) price (in eur).

**Data Cleaning:**

Cleaning Criteria:
- Removed entries with unusually repetitive or unrealistic pricing
- Excluded cars with implausible manufacturing years or mileage values

Final Sample: 1,114,112 rows used for analysis

**Selection Criteria: Identifying Top Luxury Used Models**

1. Core Buyer Metrics: Price, Mileage, Fuel, Year
​
Price and fuel economy consistently rank as top purchase factors among car buyers. Since engine displacement directly influences fuel consumption, this analysis prioritizes vehicles with engine sizes under 2,000 ccm—supporting fuel efficiency and suitability for commuting within the Greater Toronto Area.

Mileage and manufacture year are also crucial indicators of performance, reliability, and long-term maintenance costs. Cars with low mileage and newer model years are more attractive to buyers seeking a balance of affordability and durability.

​Detailed selection thresholds for these criteria are shown in Table 1, including benchmarks for luxury brand designation, model year cutoffs, and engine size.

<img src="images/Table 1.png" width="550"/>

2. Market Saturation & Sales Benchmark: Evaluating Used Car Ad Volume
​
The dealership’s sales goal is 10 cars per day, operating six days per week—totaling 3,180 vehicles per year. To meet this target, selected models must appear frequently across listings.

A higher volume of used car ads suggests stronger popularity and buyer interest, which also reflects potentially desirable features and brand trust. Therefore, quantity of ads is treated both as a performance proxy and a filter for viable sales candidates.

## Execution Steps
- Load the dataset into the Hadoop Distributed File System (HDFS)
- Launch the Hive shell environment
- Create a database and the required table(s)
- Perform data cleaning using HiveQL queries
- Conduct data analysis within the Hive shell

View [Execution Screenshot](./docs/execution_screenshot.pdf)

## Main Findings

1. Primary Selection Results​

A total of 29 car models (based on 7,241 ads) met the initial selection criteria:
- Luxury brands
- Manufactured after 2011
- Mileage under 16,093 KM
- Engine displacement of 2,000 ccm or less

The selected cars showed:
- Price range: $16,683 to $33,417
- Mileage range: 10,667 KM to 15,750 KM
- Engine size: 647 to 1,997 ccm
  
Brand distribution within this pool revealed a heavy skew toward Audi, which accounted for 85% of qualifying ads. Volvo followed with 8%, and BMW with 7%. Other luxury brands such as Lexus and Mercedes-Benz had very few listings that met the criteria (see Figure 3).

<img src="images/Figure 3.png" width="550"/>

2. Secondary Selection Strategy
​​
To identify the final Top 15 luxury used cars most likely to drive dealership sales, a two-step approach was applied:
- Step 1: Selected 10 models with the highest number of qualifying ads—indicating robust availability and consumer preference.
- Step 2: Added 5 additional models representing different brands or segments to diversify offerings.

Final selection highlights (based on the 15 top luxury models listed in Table 4):
- Audi A3: Most frequently listed model
- Audi A1: Lowest average price
- Audi Coupe: Lowest average mileage
- Mercedes-Benz Vito: Lowest engine displacement

<img src="images/Table 4.png" width="550"/>

These models stood out across different dimensions—availability, affordability, condition, and engine size—offering a versatile mix for strategic dealership focus.

​Notably, five models with lower ad volumes—Volvo XC60, BMW Z4, Volvo V70, Lexus CT-200H, and Mercedes-Benz Vito—may be perceived as more exclusive. These could be strategically featured in dealership marketing campaigns to underscore scarcity and uniqueness.

## Conclusion
This analysis identifies key performance traits and ad volume trends to recommend 15 high-potential luxury used car models tailored for young professionals in Toronto’s financial district. The dealership can leverage these insights to align inventory and marketing with buyer preferences, maximize sales, and differentiate its brand in a competitive post-pandemic market.
