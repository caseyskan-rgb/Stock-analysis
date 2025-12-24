# Stock-analysis
This project analyzes stocks by identifying the strongest companies relative to peers and determining optimal buy timing. It uses price trends and quarterly financial statements benchmarked against peer metrics, with the goal of incorporating machine learning to generate buy/sell signals and build stock portfolios across industries.

==============================================
Quick Summary: 
The system analyzes stocks by grouping companies by industry, sub-industry, and market capitalization to ensure fair comparisons. Financial data is scraped from SEC filings and transformed into key financial metrics, ratios, and growth measures.

Companies are benchmarked against peers within the same market-cap group, scored using a weighted valuation framework, and ranked to identify the strongest performers. Industry and sub-industry price trends are analyzed separately to detect broader market conditions and potential breakout behavior.

Planned extensions include combining valuation scores with industry-level price trend signals to dynamically weight fair value versus momentum. These scores will be used to rank top candidates across sub-industries and construct diversified portfolios based on user-defined industry allocations. Future versions will integrate machine learning models to improve buy/sell timing decisions.
===============================================


*The section below provides a more detailed breakdown of the system’s internal logic and design decisions*


Expanded Summary (Technical Breakdown – Optional):
1. The code scrapes financial data from SEC filings using an API key.
2. The system independently processes each sub-industry and, within each sub-industry, sorts companies based on market capitalization.
3. Stocks are categorized prior to execution. Each company within an industry is separated into:
- (A) a sub-industry category (where 75% or more of the company’s revenue originates from that sub-industry), and
- (B) three to four market-cap groupings to further segment the sub-industry.
4. The sub-industry group (A) is used to determine price trends and overall market conditions within both the industry and sub-industry (bullish or bearish), allowing the system to identify potential breakouts before they become widely recognized.
5. The market-cap group (B) is used to determine the strongest stocks relative to their direct competitors, leveraging the scraped financial data.
6. Financial metrics (e.g., revenue, net income, etc.) extracted from financial statements are stored programmatically.
7. These stored values are processed through a series of equations to compute financial ratios and compound annual growth rates (CAGR).
8. Using these computed values, benchmarks are created within each market-cap group to determine whether a stock is outperforming or underperforming its average competitors, depending on the intended purpose of each metric.
9. Selected benchmark ratios and CAGR values are evaluated using a Fair Value Screener consisting of seven weighted metrics. Each metric contributes to a cumulative score based on whether it exceeds or falls below its benchmark, with weighting determined by relative importance.
10. Stocks are ranked based on their total valuation score, with rank #1 representing the highest-performing stock and the lowest rank representing the weakest performer.

Future Plans:
1. From each market-cap group, the top two stocks will be selected and scored out of 100 based on their fair valuation results.
2. A secondary valuation score will be assigned based on price trends within the corresponding sub-industry, reflecting capital inflows or outflows.
3. If both the industry and sub-industry trends are neutral, the total score will be weighted 50% toward price trends and 50% toward fair value.
4. If the industry and sub-industry indicate a bullish trend or emerging momentum, price trends will account for 60% of the total score and fair value for 40%.
5. If the industry and sub-industry indicate a bearish trend or potential decline, price trends will account for 40% of the total score and fair value for 60%.
6. The top-performing stocks from each sub-industry will then be compared across industries to generate a ranked list of the strongest stocks to buy at the current market position.
7. Finally, the user will choose what percentage of stocks they want in each industry, depending on risk-tolerance and capital constraints, creating a well-diversified stock portfolio.
