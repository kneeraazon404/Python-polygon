# Python--polygon

## The main aspects of the task are as follows

### main_sc.py

Acts as the entry point of the application, setting up date parameters, tasks, and other initializations.

### parabscanner.py

Responsible for executing scanner operations, including data filtering and sorting to produce the final output. This file also includes attempts to integrate market capitalization data, which have been noted as inefficient, especially when deployed on AWS.

### scanner.py

Defines an abstract scanner class that outlines the basic structure for scanner operations.

### ticker_sc.py

Handles data queries to Polygon.io, including data cleanup tasks. This file is identified as the key area for enhancement, particularly for integrating market capitalization data fetched from Polygon.io into the scanner's workflow.

The job involves refining the logic in ticker_sc.py to efficiently pull ticker details, including market capitalization, from Polygon.io, and then incorporating this data into parabscanner.py to enable filtering and sorting by market cap in the final results. The aim is to follow existing code patterns, especially the approach used in ticker_sc.py for data retrieval, to ensure that data queries to Polygon.io are minimized, thus avoiding multiple requests for the same information.

## Key Requirements and Clarifications

Enhance ticker_sc.py to efficiently integrate market capitalization data from Polygon.io.

Improve the integration in parabscanner.py to filter and sort by market cap more effectively, addressing the inefficiency and performance issues noted, especially for deployment on AWS.

Follow the existing code patterns, particularly for data retrieval in ticker_sc.py, to ensure efficient and minimal querying to Polygon.io.
