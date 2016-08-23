Gaia Geotrellis Plugin
=======================

Run Geotrellis processes from Gaia.  Based on https://github.com/OpenGeoscience/nex/tree/master/geotrellis-pipeline by @dorukozturk

# Instructions
1) Install the plugin
```
pip install -e .
```

2) Install sbt

```
cd geotrellis
sh
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823
sudo apt-get update
sudo apt-get install sbt
```

3) Build the fat geotrellis jar
``` sh
cd geotrellis
sbt assembly
```

