var tar = require('tar')
var fs = require('fs')
var fsPromises = fs.promises
var Dat = require('dat-node')
var crypto = require('crypto')
var rmdir = require('rimraf');

const ManifestVersion = 2
const ManifestType = "application/vnd.docker.distribution.manifest.v2+json"
const ConfigType = "application/vnd.docker.container.image.v1+json"
const LayerType = "application/vnd.docker.image.rootfs.diff.tar.gzip"

/**
 * Return a stream on the file given in argument, or on stdin if not specified
 */
function getInput() {
  let fileName;
  // first two elements are node and js file name
  if (process.argv.length > 2) {
    fileName = fs.createReadStream(process.argv[2])
  }
  else {
    fileName = process.stdin
  }
  
  return fileName;

}

/**
 * Untar the given stream to a target directory
 * @param {stream} input Stream from which to untar
 * @param {string} newDir Destination dir
 */
function untar(input, newDir) {
  input.pipe(
    tar.x({
      C: newDir
    })
  )

  return new Promise( (resolve, reject) => {
    input.on('end', () => {
      resolve();
    });
  })
}

/**
 * 
 * @param {string} sourceTempDir Dir from which to read the file
 * @param {string} file Name of the json file to read
 */
async function parseJsonFromFile(sourceTempDir, file) {
  let content = await fsPromises.readFile(`${sourceTempDir}/${file}`)

  return JSON.parse(content)
}

/**
 * Open the file repositories in the given docker save structure
 * and parse its Json content
 * 
 * @param {string} sourceTempDir Directory containing the structure extracted from docker save
 */
async function parseRepositories(sourceTempDir) {
  return parseJsonFromFile(sourceTempDir, 'repositories')
}

/**
 * Open the file manifests.json in the given docker save structure
 * and parse its Json content
 * 
 * @param {string} sourceTempDir Directory containing the structure extracted from docker save
 */
async function parseManifest(sourceTempDir) {
  return parseJsonFromFile(sourceTempDir, 'manifest.json')

}

/**
 * Remove the host and port from the given image name
 * @param {string} imageName 
 */
function simplifyName(imageName) {
  let lastSlashPos = imageName.lastIndexOf('/')

  if (lastSlashPos < 0) {
    // no need to simplify
    return imageName
  }

  let firstPart = imageName.substr(0, content)

  if (firstPart.indexOf('.') > -1 || firstPart.indexOf(':')) {
    // the first part of the name is a host. Return what is after
    return imageName.substr(content + 1)
  }

  return imageName

}

/**
 * Create a directory structure, by creating a manifests and blobs directory
 * inside a folder named with the image name
 * 
 * @param {string} dir Destination dir where to create the directory structure
 * @param {string} name Name of the image
 */
async function createDirectoryStructure(dir, name) {
  await fsPromises.mkdir(`${dir}/work/${name}/manifests`, {recursive: true})
  await fsPromises.mkdir(`${dir}/work/${name}/blobs`, {recursive: true})
}

/**
 * Copy the root layer, i.e. the json file containing the description
 * of the image
 * 
 * @param {object} sourceManifest Manifest parsed from the docker save dir
 * @param {string} imageName Name of the processed image
 * @param {string} sourceDir Path to the docker extract directory
 * @param {string} destDir Path to the target dat archive
 * @returns {object} The JSON structure representing the manifest
 */
async function copyRootLayer(sourceManifest, imageName, sourceDir, destDir) {
  let config = sourceManifest['Config']

  let rootLayerSha = config.substr(config, config.length - 5)

  let destFile = `${destDir}/work/${imageName}/blobs/sha256:${rootLayerSha}`

  await fsPromises.rename(`${sourceDir}/${config}`, destFile)

  return {
    SchemaVersion: ManifestVersion,
    MediaType:     ManifestType,
    Config: {
      MediaType: ConfigType,
      Digest:    `sha256:${rootLayerSha}`,
      Size:      await fsPromises.stat(destFile).size,
    },
    Layers: []
  }

}

/**
 * Compress a specific layer, compute it's sha256 hash and return
 * a layer object that will be part of the target manifest
 * 
 * @param {object} layerJson The prepared object that will describe the layer in the target manifest
 * @param {string} layerFile Name of the layer file in the source directory
 * @param {string} sourceDir Source dir from which to process the layer file
 * @param {string} destDir Destination dir to which copy the layer file
 * @param {string} imageName Name of the image
 */
function compressLayer(layerJson, layerFile, imageName, sourceDir, destDir) {
  
  let slashPos = layerFile.indexOf('/')
  let sha = layerFile.substring(0,slashPos)

  let destTempFile = `${destDir}/${sha}.tmp`

  let sourceFile = `${sourceDir}/${layerFile}`

  let sourceStream = fs.createReadStream(sourceFile)

  let destStream = fs.createWriteStream(destTempFile)

  let sha256Digester = crypto.createHash("sha256")

  let promise = new Promise( (resolve, reject) => {
    sourceStream.on('end', async () => {
      let sha = sha256Digester.digest('hex')

      let destFileName = `${destDir}/work/${imageName}/blobs/sha256:${sha}`

      await fsPromises.rename(destTempFile, destFileName)
      
      layerJson.Size = await fsPromises.stat(destFileName).size
      layerJson.Digest = `sha256:${sha}`

      resolve(layerJson)
    })
  })

  sourceStream.pipe(destStream)
  sourceStream.pipe(sha256Digester)

  return promise
}

/**
 * Process other layers by gzipping them, add them to the target directory
 * and compute the resulting layer substructure
 * 
 * @param {object} manifest The prepared manifest object that will be written in the dest directory
 * @param {string} imageName The name of the image
 * @param {array} layers List of layers to process
 * @param {string} sourceDir Source directory where to find layers to process
 * @param {string} destDir Destination directory where to put the new layer
 */
function copyOtherLayers(manifest, imageName, layers, sourceDir, destDir) {
  let allPromises = layers.map( async l => {
    console.log(`Processing layer ${l}`)
    layerJson = {
      MediaType: LayerType
    }

    manifest.Layers.push(layerJson)
    
    await compressLayer(layerJson, l, imageName, sourceDir, destDir)
  })

  return Promise.all(allPromises)
}

/**
 * Write the given manifest object in the target directory
 * 
 * @param {object} manifest The json object containing the manifest
 * @param {string} imageName Name of the image
 * @param {string} destDir Working directory where to write the manifest
 */
async function writeManifest(manifest, imageName, destDir) {
  let destFile = `${destDir}/work/${imageName}/manifests/latest-v2`
  fsPromises.writeFile(destFile, JSON.stringify(manifest))
} 

async function createDatRepo(destDir) {

  Dat(`${destDir}/work`, function (err, dat) {
    if (err) throw err
    // nothing specific to do, the dat file will be created
  });
}

const run = async () => {
  let input = getInput()

  let sourceTempDir = await fsPromises.mkdtemp("image2dat_src_")

  await untar(input, sourceTempDir)

  let destTempDir = await fsPromises.mkdtemp("image2dat_dest_")

  let repositories = await parseRepositories(sourceTempDir)

  let image = Object.keys(repositories)[0]
  let tag = repositories[image]

  let name = simplifyName(image)

  await createDirectoryStructure(destTempDir, name)

  let sourceManifest = await parseManifest(sourceTempDir)

  let firstManifest = sourceManifest[0]

  let manifest = await copyRootLayer(firstManifest, name, sourceTempDir, destTempDir)

  let layers = firstManifest['Layers']

  await copyOtherLayers(manifest, name, layers, sourceTempDir, destTempDir)

  await writeManifest(manifest, name, destTempDir)

  await createDatRepo(destTempDir)

  rmdir(sourceTempDir, (error) => {
    if (error) {
      console.log("WARN: can't delete source temporary directory")
    }
  });

  return `${destTempDir}/work`

}

run().then( (path) => {
  console.log(`Done! The dat repository is in ${path}`)
}).catch(e => {
  console.log(e.stack)
});