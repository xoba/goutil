package goutil

import (
	"fmt"
)

type Replicator struct {
}

func (Replicator) Name() string {
	return "replicate,replicate a system"
}

func (r Replicator) Run(args []string) {
	fmt.Println("hello world... t.b.d.")
}

const SCRIPT = `#!/bin/bash

cd /root

locale-gen en_US.UTF-8

# make sure required packages are installed
aptitude update && aptitude install -y gcc libc6-dev git emacs mercurial golang-mode ntp python-pip curl

# setup ssh....

# make sure time is set correctly
service ntp stop && ntpd -gq && service ntp start

# take a file system inventory
updatedb

# obtain latest and greatest go-lang build:
curl $(curl http://************************/stable.txt) > go.tar.gz && rm -rf go && tar xf go.tar.gz

# goroot and add go/bin to path (need base64 to avoid shell escaping?)
echo -n "ZXhwb3J0IEdPUk9PVD0vcm9vdC9nbwpleHBvcnQgUEFUSD0kUEFUSDokR09ST09UL2Jpbgo=" | base64 -d >> .bashrc

# clone the repo
git clone --recursive ***********
cd ***

# emacs hook to format all go files before saving:
cat emacs.txt >> ~/.emacs

./install.sh

# convenient aws command-line app
pip install awscli
mkdir ~/.aws
cp lib/awsconfig ~/.aws/config

`
