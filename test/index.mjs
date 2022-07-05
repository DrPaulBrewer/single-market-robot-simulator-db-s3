/* eslint-env node, mocha */

// Copyright 2022 Paul Brewer
// Economic and Financial Technology Consulting LLC
// Open Source License: MIT License

// import assert from 'assert';
import 'should';
import {S3BucketDB} from '../src/index.mjs';
import fs from 'fs';

const s3JsonFilename = './s3.json';
const s3Json = JSON.parse(fs.readFileSync(s3JsonFilename));
const configJsonFilename = 'test/data/Intro-1-ZI-Agent-Vary-Number-of-Buyers/config.json'
const expectedConfigJson = JSON.parse(fs.readFileSync(configJsonFilename));
const latin1 = {encoding: 'latin1'};

let s3DB, folders;

describe('single-market-robot-simulator-db-s3', function(){

  describe('listStudyFolders', function(){

      it('S3BucketDB initializes correctly', function(){
          s3DB = new S3BucketDB(s3Json);
      });

      it('listStudyFolders() should find Intro-1-ZI-Agent-Vary-Number-of-Buyers and no-zips-yet', async function(){
        folders = await s3DB.listStudyFolders();
        folders.length.should.equal(2);
        folders[0].name.should.equal('Intro-1-ZI-Agent-Vary-Number-of-Buyers');
        folders[1].name.should.equal('no-zips-yet');
      });
      it('listStudyFolders("no-such-folder") should return empty array []', async function(){
          const folder = await s3DB.listStudyFolders('no-such-folder');
          folder.should.deepEqual([]);
      });
      it('listStudyFolders("Intro-1-ZI-Agent-Vary-Number-of-Buyers") should find 1 folder', async function(){
         const folder = await s3DB.listStudyFolders("Intro-1-ZI-Agent-Vary-Number-of-Buyers");
         folder.length.should.equal(1);
      });
  });
  describe(`test first folder `, function(){
     it('folder.search() yields 2 files', async function(){
         const files = await folders[0].search();
         files.should.deepEqual([
             {name: '20201004T001600.zip', size: 34580297},
             {name: 'config.json', size: 1529}
         ]);
     });
     it('folder.search("config.json") yields 1 file', async function(){
        const files = await folders[0].search('config.json');
        files.should.deepEqual([
            {name: 'config.json', size: 1529}
        ]);
     });
     it('folder.search("nope") yields empty array', async function(){
        const files = await folders[0].search('nope');
        files.should.deepEqual([]);
     });
     it('folder.download({name:"config.json"}) succeeds and matches test file', async function(){
        const configJSON = await folders[0].download({name: 'config.json'});
        configJSON.should.deepEqual(expectedConfigJson);
     });
     it('folder.download({name:"bull.crap"}) rejects', async function(){
        async function bad(){
            return folders[0].download({name:"bull.crap"});
        }
        return bad().should.be.rejected();
     });
     it('folder.download({name:"bull.json"}) rejects', async function(){
        async function bad(){
            return folders[0].download({name:"bull.json"});
        }
        return bad().should.be.rejected();
     });
     it('folder.download() rejects', async function(){
         return folders[0].download().should.be.rejected();
     });
     it('folder.download({}) rejects', async function(){
          return folders[0].download({}).should.be.rejected();
     });
     it('folder.upload config.json rejects with Policy Violation', async function(){
         const options = {
             name: 'config.json',
             contents: {
                 name: 'test123'
             }
         };
         return folders[0].upload(options).should.be.rejectedWith(/Policy Violation/);
     });
     it('folder.upload config.json should succeed', async function(){
        const newFolder = s3DB.newFolder('no-zips-yet');
        const options = {
             name: 'config.json',
             contents: {
                 name: 'test123'
             }
        };
        await newFolder.upload(options);
     });
  });
});

