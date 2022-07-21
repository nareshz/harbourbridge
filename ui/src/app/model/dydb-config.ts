export default interface DydbConfig {
    dbEngine: string
    awsAccessKeyID: string
    awsSecretAccessKey: string
    awsRegion: string
    dydbEndpoint: string,
    schemaSampleSize: string,
}