all:
	cp seedfile.txt seeds
	python3 twaler.py config.txt

quota:
	curl http://twitter.com/account/rate_limit_status.json

clean:
	rm -rf cache log seedsdone processed_crawl seeds/* *.pyc
