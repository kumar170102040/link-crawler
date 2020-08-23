# python-web-crawler
A spiderbot (web scraper) that continuously runs in the background and recursively scrapes all links it can find,

The Root URL for this project is https://flinkhub.com . The Root URL has been manually entered into the database before the process starts.

As soon as the process starts, it checks the “links” table for pending links to scrape. It scrape all these links, extract all valid links (through <a> tags) from each of these pages, and save them in the database. It also save the response HTML on the disk as a file with a random file name. This is considered as one scraping cycle. The process will then start the next cycle of scraping. The process sleep for 5 seconds between each cycle of scraping.
  
The process is written inside an infinite loop i.e. it never end and should start a new cycle 5 seconds after last cycle is complete.
The process does not scrape links that are scraped already in the last 24 hours. The process implements multithreading with 5 threads running parallelly i.e. it is crawling
up to 5 links in parallel. The process never crashes and kill itself due to run-time errors. 

If all links have been scraped in last 24 hours and there are no new links, the process just prints “All links crawled” and consider that cycle as completed.
For the confines of this project, we have limited maximum number of links to 5000.
