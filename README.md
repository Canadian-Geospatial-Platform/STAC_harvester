# STAC_harvester
Harvesting STAC endpoints for geo.ca

# Deployment as a zip using pip

Use `pip install` and target the `requirements.txt` to install the libraries inside of a new folder (say 'stac-harvest') and run the following commands.

Note: the app.py and any source code must appear in the root of the zip package. 

For more information, refer to the [packaging documentation on AWS](https://docs.aws.amazon.com/lambda/latest/dg/python-package.html#python-package-create-package-with-dependency)

```
cd stac-harvester
pip install -t stac-harvest/ -r requirements.txt
cd stac-harvest
zip -r stac-harvest-20220715.zip ../app.py ../__init.py__ ./*
```

