all:
	cp seed.lst seeds
	python2.6 twaler.py

quota:
	curl http://twitter.com/account/rate_limit_status.json

clean:
	rm -rf cache log seedsdone processed_crawl seeds/* *.pyc
