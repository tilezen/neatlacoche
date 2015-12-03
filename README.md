# Neatlacoche

Neatlacoche is a tool for splitting the
[OpenStreetMap history planet](http://planet.openstreetmap.org/planet/full-history/)
into tiles, which are known as "Non-Editorialised Analysis Tiles" or NEAT tiles.

## Motivation

The latest history planet, at the time of writing, is 48GB in PBF format. That's
a _lot_ of data! While having a lot of data is great, it makes it really, really
hard to process and to develop against.

For smaller extracts, there are the
[Mapzen metro extracts](https://mapzen.com/data/metro-extracts),
[other extracts](http://wiki.openstreetmap.org/wiki/Planet.osm#Country_and_area_extracts),
and [MaZderMind's history extracts](http://osm.personalwerk.de/full-history-extracts/),
which are all great projects and really useful for getting a subset of the data
to work with on a local machine. However, they are not so useful for performing
global analysis piecewise. The concept of tiles can be helpful here, just as the
illusion of a seamless, global image can be created using raster or vector
tiles, NEAT tiles is an attempt to create the illusion of a seamless, global
data set.

Display tiles are loaded on-demand by the client displaying them, and may even
be rendered on-demand from a server which contains all the data. But this server
is often highly specialised and it's difficult to build extra processing steps
on top of its output as it may have already discarded or processed the data in
such a way that the original is not recoverable. NEAT tiles tries to extend the
idea of tiles all the way down to the original, "non-editorialised" data, in
such a way that anyone can build on-demand processing and analysis layers on top
of it.

Having said all that, the current state is non-working, work-in-progress,
pre-alpha! But if the idea sound exciting, join us - we welcome your
contribution.

## Installation

You will need some dependencies:

```
go get github.com/gogo/protobuf/proto
go get github.com/gogo/protobuf/protoc-gen-gogo
go get github.com/gogo/protobuf/gogoproto
go get github.com/syndtr/goleveldb/leveldb
go get github.com/paulmach/go.geo
```

But then you should be able to:

```
go install github.com/mapzen/neatlacoche
```

And the `bin/neatlacoche` binary should be built. If you encounter any
difficulties, please let us know on the
[issues page](https://github.com/mapzen/neatlacoche/issues).

## Contributing

If you find an issue, please let us know by filing it on the
[issues page](https://github.com/mapzen/neatlacoche/issues). If you'd like to
contribute an improvement, please send us a Pull Request!

## License

Copyright Mapzen 2015. Released under the MIT license, please see COPYING.md for
details.

## Naming

The name is a play on [huitlacoche](https://en.wikipedia.org/wiki/Corn_smut), a
Mexican delicacy, and "NEAT" tiles.
