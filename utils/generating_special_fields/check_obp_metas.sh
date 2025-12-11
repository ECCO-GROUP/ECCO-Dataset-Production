#!/bin/bash

for i in `find . |grep 000732.meta |grep meta`; do echo " "; echo "$i"; cat $i |grep prec; done


