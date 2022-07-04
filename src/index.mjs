/* Copyright 2022- Paul Brewer, Economic and Financial Technology Consulting LLC */
/* This file is open source software.  The MIT License applies to this software. */

import { S3, ListObjectsCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { expectSafeObject, StudyFolder } from "single-market-robot-simulator-db-studyfolder";

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
    const response = await s3Client.send(command);
    let { Contents, IsTruncated, NextMarker } = response;
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
}

export class StudyFolderForS3 extends StudyFolder {
  #s3Client;
  #bucket;
  #list;

  constructor(options){
    super({
      name: options.name,
      size: options?.size,
      dated: options?.dated
    });
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
      let s3Response;
      try {
        s3Response = await this.#s3Client.send(new GetObjectCommand(obj));
      } catch(e){
        if (e?.Code!=='NoSuchKey')
          console.log(e);
      }
      if (s3Response?.$metadata?.httpStatusCode===200){
        // Don't reinvent wheel. Let the Response API in the browser/node18 handle it from here.
        const response = new Response(s3Response.Body);
        const result = (method===null)? response: await response[method]();
        if (typeof(result)==='object')
          expectSafeObject(result);
        return result;
      }
      throw new Error(`download failed for ${name}`);
    }
    throw new Error(`download unimplemented for ${name}`);
  }
}
