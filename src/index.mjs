/* Copyright 2022- Paul Brewer, Economic and Financial Technology Consulting LLC */
/* This file is open source software.  The MIT License applies to this software. */

import { XMLParser } from 'fast-xml-parser';
import { S3, ListObjectsCommand, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { Upload } from "@aws-sdk/lib-storage";
import { expectSafeObject, StudyFolder } from "single-market-robot-simulator-db-studyfolder";

const XML = new XMLParser();

const handlers = [
  ['json','json'],
  ['txt','text'],
  ['md','text'],
  ['zip','arrayBuffer']
];

function s3Lister(s3Client,bucket){
  return async function ({prefix,map,filter}){
    const maptype = typeof(map);
    const filtertype = typeof(filter);
    if (maptype!=='function')
        throw new Error(`Expected map to be a function, got:${maptype}`)
    const results = [];
    const input = { Bucket: bucket };
    if (prefix) input.Prefix = prefix;
    const command = new ListObjectsCommand(input);
    const signedUrl = await getSignedUrl(s3Client, command, {expiresIn: 60});
    const response = await fetch(signedUrl);
    const responseText = await response.text();
    const responseObj = XML.parse(responseText);
    let { Contents, IsTruncated, NextMarker } = responseObj?.ListBucketResult;
    if ((typeof(Contents)==='object') && !(Array.isArray(Contents))) Contents=[Contents];
    if (Contents===undefined) Contents = [];
    if (!Array.isArray(Contents) || (Contents.length===0))
      return [];
    if (filtertype==='function')
      Contents = Contents.filter(filter);
    Contents.forEach((content)=>{
      const result = map(content);
      results.push(result);
    });
    if (IsTruncated) console.warn("listing more than 1000 objects unimplemented", NextMarker);
    return results;
  };
}

export class S3BucketDB {
  #s3Client;
  #bucket;
  #list;

  /**
   * Create a new S3BucketDB
   *
   * @param options
   * @param {string} options.endpoint s3 endpoint
   * @param {string?} options.region s3 region
   * @param {string} options.bucket s3 bucket name
   * @param {string} options.a s3 access key id
   * @param {string} options.s s3 secret key
   * @returns {Promise<Any>}
   */
  constructor(options){
    const s3Options = {};
    ['endpoint','region'].forEach((k)=>{
      if ((typeof(options[k])==='string') && (options[k].length>0))
        s3Options[k] = options[k];
    });
    const credentials = {
      accessKeyId: options.a,
      secretAccessKey: options.s
    };
    s3Options.credentials = credentials;
    this.#s3Client = new S3(s3Options);
    this.#bucket = options.bucket;
    this.#list = s3Lister(this.#s3Client,this.#bucket);
  }

  async listStudyFolders(name){
    const bucket = this.#bucket;
    const s3Client = this.#s3Client;
    const list = this.#list;
    const listOptions = {
      filter: ({Key}) => (
          Key?.endsWith("/config.json")
      ),
      map: (content) => {
        const name = content.Key.replace(/\/config.json$/, '');
        const size = +content.Size;
        const dated = new Date(content.LastModified);
        const subList = ({map, filter, prefix}) => (list({
          prefix: name + '/' + ((prefix?.length>0)? prefix: ''),
          map,
          filter
        }));
        return new StudyFolderForS3({name, size, dated, bucket, s3Client, list: subList});
      }
    };
    if (name){ listOptions.prefix = name+'/'; }
    return this.#list(listOptions);
  }

  newFolder(name){
    const s3Client = this.#s3Client;
    const bucket = this.#bucket;
    const list = this.#list;
    const subList = ({map, filter, prefix}) => (list({
      prefix: name + '/' + ((prefix?.length>0)? prefix: ''),
      map,
      filter
    }));
    return new StudyFolderForS3({
      name,
      s3Client,
      bucket,
      list: subList
    })
  }
}

export class StudyFolderForS3 extends StudyFolder {
  #s3Client;
  #bucket;
  #list;

  constructor(options){
    super({name: options.name});
    this.#s3Client = options.s3Client;
    this.#bucket = options.bucket;
    this.#list = options.list;
  }

  async search(prefix) {
    const listOptions = {
      map({Key, Size}) {
        const fileParts = Key.split('/');
        const name = fileParts[fileParts.length - 1];
        return {name, size: Size}
      }
    }
    if (prefix) listOptions.prefix = prefix;
    return this.#list(listOptions);
  }

  async download({name}){
    if (typeof(name)!=='string') throw new Error("name[string] required");
    const pair = handlers.find(([ext])=>(name.endsWith(ext)));
    if (pair){
      const [ext, method] = pair; // eslint-disable-line no-unused-vars
      const obj = {
        Bucket: this.#bucket,
        Key: `${this.name}/${name}`
      };
      let response;
      try {
          const command = new GetObjectCommand(obj);
          const signedUrl = await getSignedUrl(this.#s3Client, command, {expiresIn: 60});
          response = await fetch(signedUrl);
      } catch(e){
          console.log(e);
      }
      if (response?.ok){
        const result = (method)? (await response[method]()): response;
        if (typeof(result)==='object')
          expectSafeObject(result);
        return result;
      }
      throw new Error(`download failed for ${name}`);
    }
    throw new Error(`download unimplemented for ${name}`);
  }

  async upload(options){
    await this.prepUpload(options);
    const {name, blob } = options;
    const params = {
      Bucket: this.#bucket,
      Key: `${this.name}/${name}`,
      Body: await blob
    };
    const parallelUploads = new Upload({
      client: this.#s3Client,
      params,
      queueSize: 4, // optional concurrency configuration
      partSize: 1024 * 1024 * 5, // optional size of each part, in bytes, at least 5MB
      leavePartsOnError: false, // optional manually handle dropped parts
    });

    parallelUploads.on("httpUploadProgress", (progress) => {
      console.log({ source:'upload', progress});
    });

    await parallelUploads.done();
  }
}
