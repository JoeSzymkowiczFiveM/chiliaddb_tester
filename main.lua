local function getTotalRecordsCount(data)
    local size = 0
    for _ in pairs(data) do
        size = size + 1
    end
    return size
end                                              

ChiliadDB.ready(function()
    ChiliadDB.dropCollection('chiliad_tester')
    ChiliadDB.dropCollection('sdfgsdfgsdf')
    ChiliadDB.dropCollection('table_tester')
    ChiliadDB.dropCollection('test44')
    ChiliadDB.dropCollection('multi_tester_renamed')
    print("ChiliadDB Tester - ^2STARTING^7")

    print("Test 1 : insert - ^3RUNNING^7")
    local id1 = ChiliadDB.insert({collection = 'chiliad_tester', document = {name = "test1", age = 10}, options = {selfInsertId = 'testId'}})
    local id2 = ChiliadDB.insert({collection = 'chiliad_tester', document = {name = "test2", age = 10}, options = {selfInsertId = 'testId'}})
    local id3 = ChiliadDB.insert({collection = 'chiliad_tester', document = {name = "test70", age = 10}, options = {selfInsertId = 'testId'}})
    ChiliadDB.update({collection = 'chiliad_tester', query = {name = "test1"}, update = {name = "test69"}})
    ChiliadDB.update({collection = 'chiliad_tester', query = {id = id2}, update = {name = "test70", age = 11}})
    local results3 = ChiliadDB.find({collection = 'chiliad_tester', query = {testId = id1}})
    local results3count = getTotalRecordsCount(results3)
    assert(results3count == 1)
    print("Test 1 : insert - ^2PASSED^7")

    print("Test 2 : findOne - ^3RUNNING^7")
    local results2 = ChiliadDB.findOne({collection = 'chiliad_tester', query = {name = "test69"}})
    assert(results2.name == "test69")
    print("Test 2 : findOne - ^2PASSED^7")

    print("Test 3 : find 2 - ^3RUNNING^7")
    local results5 = ChiliadDB.find({collection = 'chiliad_tester', query = {name = "test69"}})
    local results5count = getTotalRecordsCount(results5)
    assert(results5count == 1)
    print("Test 3 : find 2 - ^2PASSED^7")

    print("Test 4 : exists - ^3RUNNING^7")
    local results6 = ChiliadDB.exists({collection = 'chiliad_tester', query = {name = "test69"}})
    assert(results6 == true)
    print("Test 4 : exists - ^2PASSED^7")

    print("Test 5 : exists 2 - ^3RUNNING^7")
    local results7 = ChiliadDB.exists({collection = 'chiliad_tester', query = {name = "AWEFAWEFAWEFAWEFAWSEF"}})
    assert(results7 == false)
    print("Test 5 : exists 2 - ^2PASSED^7")

    print("Test 6 : find 3 - ^3RUNNING^7")
    local results8 = ChiliadDB.find({collection = 'chiliad_tester', query = {name = "test69"}})
    assert(#results8 == 1)
    print("Test 6 : find 3 - ^2PASSED^7")

    print("Test 7 : find 4 - ^3RUNNING^7")
    local results9 = ChiliadDB.find({collection = 'chiliad_tester', query = {name = "test70"}})
    local results9count = getTotalRecordsCount(results9)
    assert(results9count == 2)
    print("Test 7 : find 4 - ^2PASSED^7")

    print("Test 8 : find with limit - ^3RUNNING^7")
    local results10 = ChiliadDB.find({collection = 'chiliad_tester', query = {name = "test70"}, options = {limit = 2}})
    local results10count = getTotalRecordsCount(results10)
    assert(results10count == 2)
    assert(results10[2].name == "test70")
    assert(results10[2].age == 11)
    assert(results10[1] == nil)
    print("Test 8 : find with limit - ^2PASSED^7")

    print("Test 9 : find with excludeIndexes - ^3RUNNING^7")
    local results11 = ChiliadDB.find({collection = 'chiliad_tester', query = {name = "test70"}, options = {excludeIndexes = true}})
    --we dont use getTotalRecordsCount here because the excludeIndexes param should remove holes from the indexes, see can reliably use "#" to get the count
    assert(#results11 == 2)
    print("Test 9 : find with excludeIndexes - ^2PASSED^7")

    print("Test 10 : getCurrentTableIndex - ^3RUNNING^7")
    local currentIndexCheck = 3
    local result10 = ChiliadDB.getCollectionProperties('chiliad_tester')
    assert(result10.currentIndex == currentIndexCheck)
    print("Test 10 : getCurrentTableIndex - ^2PASSED^7")

    print("Test 11 : getCurrentTableIndex 2 - ^3RUNNING^7")
    ChiliadDB.insert({collection = 'chiliad_tester', document = {name = "asdf", age = 40}})
    local result11 = ChiliadDB.getCollectionProperties('chiliad_tester')
    assert(result11.currentIndex == currentIndexCheck + 1)
    print("Test 11 : getCurrentTableIndex 2 - ^2PASSED^7")

    print("Test 12 : find 5 - ^3RUNNING^7")
    local results12 = ChiliadDB.find({collection = 'chiliad_tester'})
    assert(#results12 == 4)
    assert(results12[1].testId == 1)
    print("Test 12 : find 5 - ^2PASSED^7")

    print("Test 13 : find 5 - ^3RUNNING^7")
    ChiliadDB.delete({collection = 'chiliad_tester', query = {id = id1}})
    local results13 = ChiliadDB.find({collection = 'chiliad_tester'})
    local results13count = getTotalRecordsCount(results13)
    assert(results13count == 3)
    print("Test 13 : find 5 - ^2PASSED^7")

    print("Test 14 : find 5 - ^3RUNNING^7")
    local count = ChiliadDB.getCollectionDocumentCount('chiliad_tester')
    assert(count == 3)
    print("Test 14 : find 5 - ^2PASSED^7")

    print("Test 15 : Error Handling - ^3RUNNING^7")
    local results15 = ChiliadDB.find({table = 'asdf', options = {limit = 1}})
    assert(results15 == false)
    local error1 = ChiliadDB.replaceOne({ collection = "character_skills"})
    local error2 = ChiliadDB.insert({ collection = "character_skills"})
    local error3 = ChiliadDB.update({ collection = "character_skills"})
    assert(error1 == false)
    assert(error2 == false)
    assert(error3 == false)
    print("Test 15 : Error Handling - ^2PASSED^7")

    print("Test 16 : find with limit 2 - ^3RUNNING^7")
    local results14 = ChiliadDB.find({collection = 'chiliad_tester', options = {excludeFields = {name = true}, limit = 1}})
    local results14count = getTotalRecordsCount(results14)
    assert(results14count == 1)
    print("Test 16 : find with limit 2 - ^2PASSED^7")

    print("Test 17 : exists with lt datatype mis-match - ^3RUNNING^7")
    local resultLt = ChiliadDB.exists({collection = 'chiliad_tester', query = {age = {['$lt'] = '11'}}}) -- this should return false since 11 is a string
    assert(resultLt == false)
    print("Test 17 : exists with lt datatype mis-match - ^2PASSED^7")

    print("Test 18 : exists with lt datatype match - ^3RUNNING^7")
    local resultLt2 = ChiliadDB.exists({collection = 'chiliad_tester', query = {age = {['$lt'] = 11}}}) -- this should return false since 11 is a string
    assert(resultLt2 == true)
    print("Test 18 : exists with lt datatype match - ^2PASSED^7")

    print("Test 19 : excludeFields working - ^3RUNNING^7")
    local results19 = ChiliadDB.findOne({collection = 'chiliad_tester', query = {age = 10}, options = {excludeFields = {name = true, lastUpdated = true, testId = true}}})
    assert(results19.name == nil)
    assert(results19.lastUpdated == nil)
    assert(results19.testId == nil)
    assert(results19.age == 10)
    print("Test 19 : excludeFields working - ^2PASSED^7")

    print("Test 20 : getCollectionProperties for non-existent collection - ^3RUNNING^7")
    local results20 = ChiliadDB.getCollectionProperties('asdfasdf')
    assert(results20 == false)
    print("Test 20 : getCollectionProperties for non-existent collection - ^2PASSED^7")

    print("Test 21 : getCollectionProperties for valid collection - ^3RUNNING^7")
    local results21 = ChiliadDB.getCollectionProperties('chiliad_tester')
    assert(results21.currentIndex == 4)
    print("Test 21 : getCollectionProperties for valid collection - ^2PASSED^7")
    
    print("Test 22 : test insertMany and check output - ^3RUNNING^7")
    local insertManyIds = ChiliadDB.insertMany({collection = 'chiliad_tester', documents = {
        {name = "test70", age = 10},
        {name = "test70", age = 10},
        {name = "test70", age = 11},
        {name = "test70", age = 10},
        {name = "test70", age = 10},
        {name = "test70", age = 10},
        {name = "test70", age = 10},
    }, options = {selfInsertId = 'testId'}})
    ChiliadDB.delete({collection = 'chiliad_tester', query = {id = insertManyIds[5]}})
    ChiliadDB.delete({collection = 'chiliad_tester', query = {id = insertManyIds[6]}})
    ChiliadDB.delete({collection = 'chiliad_tester', query = {id = insertManyIds[7]}})
    ChiliadDB.insert({collection = 'chiliad_tester', document = {name = "test70", age = 10}, options = {selfInsertId = 'testId'}})
    ChiliadDB.insert({collection = 'chiliad_tester', document = {name = "test70", age = 10}, options = {selfInsertId = 'testId'}})

    local results22 = ChiliadDB.find({collection = 'chiliad_tester', query = {name = "test70"}})
    local total = getTotalRecordsCount(results22)
    assert(total == 8)
    print("Test 22 : test insertMany and check output - ^2PASSED^7")

    print("Test 23 : skipIfExists insert - ^3RUNNING^7")
    ChiliadDB.insert({collection = 'chiliad_tester', document = {name = "testexists"}, options = {skipIfExists = {name = true}}})
    ChiliadDB.insert({collection = 'chiliad_tester', document = {name = "testexists"}, options = {skipIfExists = {name = true}}})
    local results23 = ChiliadDB.find({collection = 'chiliad_tester', query = {name = "testexists"}})
    local total23 = getTotalRecordsCount(results23)
    assert(total23 == 1)
    print("Test 23 : skipIfExists insert - ^2PASSED^7")

    print("Test 24 : includeFields find - ^3RUNNING^7")
    local results24 = ChiliadDB.find({collection = 'chiliad_tester', options = {includeFields = {name = true, age = true}}})
    assert(results24[7].name ~= nil)
    assert(results24[7].age ~= nil)
    assert(results24[7].age == 11)
    assert(results24[7].testId == nil)
    print("Test 24 : includeFields find - ^2PASSED^7")

    print("Test 25 : excludeFields find - ^3RUNNING^7")
    local results25 = ChiliadDB.find({collection = 'chiliad_tester', options = {excludeFields = {name = true, age = true}}})
    assert(results25[12].name == nil)
    assert(results25[12].age == nil)
    assert(results25[12].testId ~= nil)
    print("Test 25 : excludeFields find - ^2PASSED^7")

    print("Test 26 : excludeFields find 2 - ^3RUNNING^7")
    local results26 = ChiliadDB.find({collection = 'chiliad_tester', options = {excludeFields = { age = true}, limit = 8}})
    local count = getTotalRecordsCount(results26)
    assert(count == 8)
    assert(results26[7].testId == 7)
    assert(results26[7].age == nil)
    assert(results26[13] == nil)
    print("Test 26 : excludeFields find 2 - ^2PASSED^7")

    print("Test 27 : delete many - ^3RUNNING^7")
    local results271 = ChiliadDB.find({collection = 'chiliad_tester', query = {name = "testnew"}, options = {excludeIndexes = true}})
    assert(#results271 == 0)
    local results272 = ChiliadDB.find({collection = 'chiliad_tester'})
    local results272Count = getTotalRecordsCount(results272)
    assert(results272Count == 10)
    ChiliadDB.update({collection = 'chiliad_tester', query = {testId = 5}, update = {name = "testnew"}})
    ChiliadDB.update({collection = 'chiliad_tester', query = {testId = 6}, update = {name = "testnew"}})
    local results273 = ChiliadDB.find({collection = 'chiliad_tester', query = {name = "testnew"}, options = {excludeIndexes = true}})
    assert(#results273 == 2)
    ChiliadDB.delete({collection = 'chiliad_tester', query = {name = "testnew"}})
    local results273 = ChiliadDB.find({collection = 'chiliad_tester', query = {name = "testnew"}, options = {excludeIndexes = true}})
    assert(#results273 == 0)
    local results274 = ChiliadDB.find({collection = 'chiliad_tester'})
    local results274Count = getTotalRecordsCount(results274)
    assert(results274Count == 8)
    print("Test 27 : delete many - ^2PASSED^7")

    print("Test 28 : upsert insert - ^3RUNNING^7")
    local upsetInsert= ChiliadDB.update({collection = 'chiliad_tester', query = {name = "test420420420"}, update = {name = "testupsert"}, options = {upsert = true, selfInsertId = 'testId'}})
    local upsetInsertId = upsetInsert[1]
    assert(upsetInsertId == 15)
    print("Test 28 : upsert insert - ^2PASSED^7")

    print("Test 29 : upsert update, create collection - ^3RUNNING^7")
    local upsetIdArray = ChiliadDB.update({collection = 'sdfgsdfgsdf', query = {name = "test420420420", asdf = 'Meghan'}, update = {name = "testupsert"}, options = {upsert = true, selfInsertId = 'testId'}})
    local upsetId = upsetIdArray[1]
    assert(upsetId == 1)

    local upsetIdArray2 = ChiliadDB.update({collection = 'sdfgsdfgsdf', query = {name = "testYEAH"}, update = {name = "testupsert"}, options = {upsert = true, selfInsertId = 'testId'}})
    local upsetId2 = upsetIdArray2[1]
    assert(upsetId2 == 2)

    local upsetIdArray3 = ChiliadDB.update({collection = 'sdfgsdfgsdf', query = {name = "test420420420", asdf = 'Meghan'}, update = {name = "testupsert2"}, options = {upsert = true, selfInsertId = 'testId'}})
    local upsetId3 = upsetIdArray3[1]
    assert(upsetId3 == 1)
    assert(#upsetIdArray3 == 1)
    print("Test 29 : upsert update, create collection - ^2PASSED^7")

    print("Test 30 : includeFields findOne - ^3RUNNING^7")
    local results30 = ChiliadDB.findOne({collection = 'chiliad_tester', query = {name = "test420420420"}, options = {includeFields = {name = true}}})
    assert(results30.name ~= nil)
    assert(results30.age == nil)
    assert(results30.testId == nil)
    print("Test 30 : includeFields findOne - ^2PASSED^7")

    print("Test 31 : replaceOne - ^3RUNNING^7")
    local replaceOneId = ChiliadDB.replaceOne({collection = 'sdfgsdfgsdf', query = {name = "testupsert2", asdf = 'Meghan'}, document = {name2 = 'Tater', name3 = nil}})
    local results31 = ChiliadDB.find({collection = 'sdfgsdfgsdf'})
    assert(results31[1].name2 == 'Tater')
    assert(results31[1].name == nil)
    assert(replaceOneId == 1)
    print("Test 31 : replaceOne - ^2PASSED^7")

    print("Test 32 : set key to empty table test - ^3RUNNING^7")
    local results32 = ChiliadDB.findOne({collection = 'chiliad_tester', query = {testId = 15}})
    assert(results32.stuff == nil)
    ChiliadDB.update({collection = 'chiliad_tester', query = {testId = 15}, update = {stuff = {}}})
    local results322, results322Id = ChiliadDB.findOne({collection = 'chiliad_tester', query = {testId = 15}})
    assert(results322.stuff ~= nil)
    assert(type(results322.stuff) == 'table')
    assert(#results322.stuff == 0)
    assert(results322Id == 15)
    print("Test 32 : set key to empty table test - ^2PASSED^7")

    print("Test 33 : find with exists query operator - ^3RUNNING^7")
    local result33 = ChiliadDB.find({collection = 'chiliad_tester', query = {testId = {['$exists'] = false}}})
    assert(result33[4].name ~= nil)
    print("Test 33 : find with exists query operator - ^2PASSED^7")

    print("Test 34 : findOne by id - ^3RUNNING^7")
    local result34 = ChiliadDB.findOne({collection = 'chiliad_tester', query = {id = 15}})
    assert(result34.name == 'test420420420')
    local result341 = ChiliadDB.findOne({collection = 'chiliad_tester', query = {id = 1}})
    assert(result341 == nil)
    print("Test 34 : findOne by id - ^2PASSED^7")

    print("Test 35 : collection stuff - ^3RUNNING^7")
    local result35 = ChiliadDB.createCollection('test44')
    assert(result35 == true)
    local result352 = ChiliadDB.getCollectionProperties('test44')
    assert(result352.retention == nil)
    ChiliadDB.setCollectionProperties({collection = 'test44', retention = {seconds = 5, minutes = 1}})
    ChiliadDB.insert({collection = 'test44', document = {name = "test1", age = 10}, options = {selfInsertId = 'testId'}})
    local result353 = ChiliadDB.getCollectionProperties('test44')
    assert(result353.retention == 65000)
    local result354 = ChiliadDB.getCollectionDocumentCount('chiliad_tester')
    assert(result354 == 9)
    ChiliadDB.setCollectionProperties({collection = 'test44', retention = {remove = true}})
    local result353 = ChiliadDB.getCollectionProperties('test44')
    assert(result353.retention == nil)
    print("Test 35 : collection stuff - ^2PASSED^7")

    print("Test 36 : contains - ^3RUNNING^7")
    local contains1Id = ChiliadDB.insert({collection = 'sdfgsdfgsdf', document = { stuff = {'Jeff', 'Kale', 'Al'} }})
    local contains2Id = ChiliadDB.insert({collection = 'sdfgsdfgsdf', document = { stuff = {'Tater', 'Ash', 'Sonny'} }})
    local result36 = ChiliadDB.find({collection = 'sdfgsdfgsdf', query = {stuff = {['$contains'] = 'Tater'}}})
    assert(result36[contains2Id] ~= nil)
    print("Test 36 : contains - ^2PASSED^7")

    print("Test 37 : in, nin - ^3RUNNING^7")
    local joeId = ChiliadDB.insert({collection = 'table_tester', document = {stuff = 'Joe'}})
    local daleId = ChiliadDB.insert({collection = 'table_tester', document = {stuff = 'Dale'}})
    local bobId = ChiliadDB.insert({collection = 'table_tester', document = {stuff = 'Bob'}})
    local danId = ChiliadDB.insert({collection = 'table_tester', document = {stuff = 'Dan'}})

    local result37 = ChiliadDB.find({collection = 'table_tester', query = {stuff = {['$in'] = {'Joe', 'Bob', 'Dale'}}}})
    local results37count = getTotalRecordsCount(result37)
    assert(results37count == 3)

    local result371 = ChiliadDB.find({collection = 'table_tester', query = {stuff = {['$nin'] = {'Joe', 'Bob', 'Dale'}}}})
    local result371count = getTotalRecordsCount(result371)
    assert(result371count == 1)
    print("Test 37 : in, nin - ^2PASSED^7")

    print("Test 38 : lua string match - ^3RUNNING^7")
    -- https://www.mycompiler.io/view/Bvx1KIm
    local result38 = ChiliadDB.find({collection = 'table_tester', query = {stuff = {['$match'] = '.*Da.*'}}})
    assert(result38[joeId] == nil)
    assert(result38[bobId] == nil)
    assert(result38[daleId] ~= nil)
    assert(result38[daleId].stuff == 'Dale')
    assert(result38[danId] ~= nil)
    assert(result38[danId].stuff == 'Dan')
    local result38count = getTotalRecordsCount(result38)
    assert(result38count == 2)
    print("Test 38 : lua string match - ^2PASSED^7")

    print("Test 39 : insertMany - ^3RUNNING^7")
    local ids = ChiliadDB.insertMany({collection = 'multi_tester', documents = {
        {name = "test2", age = 10, class = 'wizard'},
        {name = "test3", age = 20, class = 'warrior'}
    }, options = {selfInsertId = 'testId'}})
    assert(#ids == 2)
    assert(ids[1] == 1)
    assert(ids[2] == 2)
    print("Test 39 : insertMany - ^2PASSED^7")

    print("Test 40 : insertMany with skipIfExistsCheck - ^3RUNNING^7")
    local skipIfExistsCheck = ChiliadDB.insertMany({collection = 'multi_tester', documents = {
        {name = "test4", age = 20, class = 'warrior'},
        {name = "test4", age = 12, class = 'warrior'}
    }, options = {skipIfExists = {age = true}}})
    assert(#skipIfExistsCheck == 2)
    assert(skipIfExistsCheck[1] == false)
    assert(skipIfExistsCheck[2] == 3)
    print("Test 40 : insertMany with skipIfExistsCheck - ^2PASSED^7")

    print("Test 41 : renameCollection - ^3RUNNING^7")
    local renameResult = ChiliadDB.renameCollection({collection = 'multi_tester', newName = 'multi_tester_renamed'})
    assert(renameResult == true)
    local renameFinder = ChiliadDB.find({collection = 'multi_tester_renamed'})
    assert(#renameFinder == 3)
    print("Test 41 : renameCollection - ^2PASSED^7")

    print("Test 42 : aggregate - ^3RUNNING^7")
    ChiliadDB.insertMany({collection = 'multi_tester_renamed', documents = {
        {name = "test4", age = 20, class = 'wizard'},
        {name = "test4", age = 12},
        {name = "test4", age = 30, class = 'wizard'},
        {name = "test4", age = 45, class = 'paladin'}
    }})
    local aggregateResult = ChiliadDB.aggregate({collection = 'multi_tester_renamed', query = {name = "test4"}, group = { fields = {"name", "class"}, sum = "age", alias = "ageSum" } })
    assert(#aggregateResult == 4)
    for i = 1, #aggregateResult do
        if aggregateResult[i].class == 'wizard' then
            assert(aggregateResult[i].age == nil)
            assert(aggregateResult[i].ageSum == 50)
            assert(#aggregateResult[i].ids == 2)
        elseif aggregateResult[i].class == 'paladin' then
            assert(aggregateResult[i].ageSum == 45)
        end
    end
    print("Test 42 : aggregate - ^2PASSED^7")
end)