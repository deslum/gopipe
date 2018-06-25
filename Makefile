
.PHONY: build
build:
	cd cpipe && go build -buildmode=c-shared -o cpipe.so 
	cd cpipelib && go build -buildmode=c-shared -o cpipelib.so 
