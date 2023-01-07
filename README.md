# Bangumi Spider

This is a [scrapy](http://scrapy.org/) based spider used to scrape data from [Bangumi 番组计划](https://bgm.tv).

## What information it can scrape?

| Spider name | Purpose | Items |
|---|---|---|
| user-api | User information retrived via API. | Aligned with _User_ in [https://bangumi.github.io/api](https://bangumi.github.io/api) |
| subject-api | Subject information retrived via API. | Aligned with _Subject_ in [https://bangumi.github.io/api](https://bangumi.github.io/api) |
| collections-api | User records retrived via API. | Aligned with _UserSubjectCollection_ in [https://bangumi.github.io/api](https://bangumi.github.io/api) |
| user | User information retrived via web. | uid, name, nickname, joindate |
| record* | User-subject information retrived via web. | _uid_(user id), _iid_(item id), _typ_(item type), _state_(user's favorite state), adddate, rate, tags, comment |
| subject | Subject information retrived via web. | subjectid, subjecttype, subjectname, _order_(same as subjectid except for redirected subjects, for which original id is kept as _order_), _alias_(alias of _subjectname_), staff, _relations_(subject's relation with other subjects) |

_* record spider: it would scrape user simultaneously._

## How to use the spider?

Prerequisit of this spider is `scrapy`, so one need to install `scrapy` first.

You also need to obtain your Bangumi user token via [https://next.bgm.tv/demo/access-token]( https://next.bgm.tv/demo/access-token).

To use web spiders locally, one should git clone this repository first then run

```
scrapy crawl subject-api -a id_max=100 -a token={TOKEN}
```

`TOKEN` is the Bangumi token you retrieved via [https://next.bgm.tv/demo/access-token]( https://next.bgm.tv/demo/access-token).

`id_max` is a parameter specifying the maximun subject id it should be scraped. Meanwhile there is another parameter naming `id_min`.

For spiders scraping from web page, one need to add environmental variable `SCRAPY_PROJECT=bgm` before running. For example, on Linux it should be 

```
SCRAPY_PROJECT=bgm scrapy crawl subject -a id_max=100
```

Then you can check the scraped items under main folder.

## How to deploy?

To setup a scrapyd server, one can certainly do that by following [Scrapyd document](https://scrapyd.readthedocs.io/en/stable/). However, we are providing a docker image to help you achieve that goal more quickly. The docker image is has a nginx served as authentication server. To start the docker image, one need to specify the `USERNAME` and `PASSWORD` as environment variable.

```
docker run -d -p 6810:6810 -e USERNAME=username -e PASSWORD=password wattlebird/scrapyd:latest
curl --user username:password http://localhost:6810/schedule.json -d project=bgm -d spider=record -d id_max=100
```

Then you can visit http://localhost:6810 to watch your jobs.

The source code of that docker image is under the folder scrapyd.

To deploy bgmapi and bgm properly, one have to execute the following commands:

```
python setup_bgmapi.py bdist_egg
curl --user {USERNAME}:{PASSWORD} http://localhost:6810/addversion.json -F project=bgmapi -F version=1.0 -F egg=@dist/project-1.0-py3.10.egg # The actual egg file generated may have a differ
python setup_bgm.py bdist_egg
curl --user {USERNAME}:{PASSWORD} http://localhost:6810/addversion.json -F project=bgm -F version=1.0 -F egg=@dist/project-1.0-py3.10.egg # The actual egg file generated may have a differ
```

## Known issues

Due to sensitive content restriction, spider `subject` cannot scrape subjects that are marked as R-18.

## License

MIT Licensed.
