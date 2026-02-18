# rqutils

Greetings! Use at your own risk.

This repo holds a number of opinionated script fragments regularly used without having put them into a package "yet".
This avoids adding a huge number of (potential) dependencies.

Scripts are useful as we can "simulate" a package in any R-project by creating a DESCRIPTION file and using devtools::load_all() to load the scripts (and keeping the environment list clean(er)).
The scripts also represent the latest versions ... as these fragments developed over time ... got some level of generalisation, etc.

Opinionated := they work for me and my workflow. Thus, they may inform you (dear non-RQ reader), but there is not guarantee that this works in your environment. Nonetheless, these fragments are not secret, etc. they represent things I have to do often ... and lacked a good package/function to help me in doing this.
