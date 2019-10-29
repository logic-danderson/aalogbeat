# Aalogbeat

Welcome to Aalogbeat.

Ensure that this folder is at the following location:
`${GOPATH}/src/github.com/logic-danderson/aalogbeat`

## Getting Started with Aalogbeat

### Requirements

* [Golang](https://golang.org/dl/) 1.7

### Init Project
To get running with Aalogbeat and also install the
dependencies, run the following command:

```
make setup
```

It will create a clean git history for each major step. Note that you can always rewrite the history if you wish before pushing your changes.

To push Aalogbeat in the git repository, run the following commands:

```
git remote set-url origin https://github.com/logic-danderson/aalogbeat
git push origin master
```

For further development, check out the [beat developer guide](https://www.elastic.co/guide/en/beats/libbeat/current/new-beat.html).

### Build

To build the Linux binary for Aalogbeat run the command below. This will 
generate a binary in the same directory with the name aalogbeat.

```
make
```

To build the Windows binary for Aalogbeat run the command below. This will 
generate a Windows excutable in the same directory with the name aalogbeat.exe.

```
GOOS=windows GOARCH=amd64 go build
```

### Run

To run Aalogbeat with debugging output enabled in Linux, run:

```
./aalogbeat -c aalogbeat.yml -e -d "*"
```

To run Aalogbeat with debugging output enabled in Windows, run:

```
aalogbeat.exe -c aalogbeat.yml -e -d "*"
```

There are also PowerShell scripts install-service-aalogbeat.ps1 and 
uninstall-service-aalogbeat.ps1 that can be used to configure Aalogbeat 
to run as a Windows service.

### Test

To test Aalogbeat, run the following command:

```
make testsuite
```

alternatively:
```
make unit-tests
make system-tests
make integration-tests
make coverage-report
```

The test coverage is reported in the folder `./build/coverage/`

### Update

Each beat has a template for the mapping in elasticsearch and a documentation for the fields
which is automatically generated based on `fields.yml` by running the following command. 

```
make update
```


### Publishing Dashboards

Copy the files it generated in _meta/kibana.generated/7/dashboard to kibana/7/dashboard. 
Then run:

```
./aalogbeat setup --dashboards
```

### Exporting Dashboards

To export dashboards for inclusion into the project, run:

```
go run vendor/github.com/elastic/beats/dev-tools/cmd/dashboards/export_dashboards.go -yml dashboards.yml
```

That will update the dashboard files under _meta/kibana/7/dashboard.

Then run:

```
make update
````

Which will copy the dashboard files from _meta/kibana/7/dashboard to _meta/kibana.generated/7/dashboard.

### Cleanup

To clean  Aalogbeat source code, run the following command:

```
make fmt
```

To clean up the build directory and generated artifacts, run:

```
make clean
```


### Clone

To clone Aalogbeat from the git repository, run the following commands:

```
mkdir -p ${GOPATH}/src/github.com/logic-danderson/aalogbeat
git clone https://github.com/logic-danderson/aalogbeat ${GOPATH}/src/github.com/logic-danderson/aalogbeat
```


For further development, check out the [beat developer guide](https://www.elastic.co/guide/en/beats/libbeat/current/new-beat.html).


## Packaging

The beat frameworks provides tools to crosscompile and package your beat for different platforms. This requires [docker](https://www.docker.com/) and vendoring as described above. To build packages of your beat, run the following command:

```
make release
```

This will fetch and create all images required for the build process. The whole process to finish can take several minutes.
