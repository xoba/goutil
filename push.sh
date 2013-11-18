#!/bin/bash
export B=`uuidgen`
git checkout -b $B
git checkout master
git merge $B
git push git@github.com:xoba/goutil.git master
git pull
git branch -d $B
