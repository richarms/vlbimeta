#!groovy

@Library('katsdpjenkins') _
katsdp.killOldJobs()

katsdp.setDependencies([
    'ska-sa/katsdpdockerbase/master',
    'ska-sa/katdal/master',
    'ska-sa/katpoint/master'])
katsdp.standardBuild(push_external: true)
katsdp.mail('richarms@sarao.ac.za')
