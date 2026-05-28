if not lib.checkDependency('chiliaddb', '0.2.2', true) then return end
local testCounter = 0

local function getTotalRecordsCount(data)
    local size = 0
    for _ in pairs(data) do
        size = size + 1
    end
    return size
end

local function test(actual, expected, name, op)
    testCounter = testCounter + 1
    print(string.format("^7Test %d : %s - ^3RUNNING^7", testCounter, name))
    if op == 'toEqual' and actual == expected then
        print(string.format("^7Test %d : %s - ^2PASSED^7", testCounter, name))
    elseif op == 'toNotEqual' and actual ~= expected then
        print(string.format("^7Test %d : %s - ^2PASSED^7", testCounter, name))
    elseif op == 'toBeGreaterThan' and actual > expected then
        print(string.format("^7Test %d : %s - ^2PASSED^7", testCounter, name))
    elseif op == 'toBeLessThan' and actual < expected then
        print(string.format("^7Test %d : %s - ^2PASSED^7", testCounter, name))
    elseif op == 'toBeGreaterThanOrEqual' and actual >= expected then
        print(string.format("^7Test %d : %s - ^2PASSED^7", testCounter, name))
    elseif op == 'toBeLessThanOrEqual' and actual <= expected then
        print(string.format("^7Test %d : %s - ^2PASSED^7", testCounter, name))
    elseif op == 'toContain' and type(actual) == 'table' then
        local found = false
        for _, value in pairs(actual) do
            if value == expected then
                found = true
                break
            end
        end
        if found then
            print(string.format("^7Test %d : %s - ^2PASSED^7", testCounter, name))
        else
            print(string.format(
                "^7Assertion ^1FAILED^7 in test '%s'. Actual: %s, Expected to contain: %s, Operation: %s", name,
                tostring(actual), tostring(expected), tostring(op)))
            assert(false)
        end
    else
        print(string.format("^7Assertion ^1FAILED^7 in test '%s'. Actual: %s, Expected: %s, Operation: %s", name,
            tostring(actual), tostring(expected), tostring(op)))
        assert(false)
    end
end

local function runAssertTest(name, cb)
    testCounter = testCounter + 1
    print(string.format("^7Test %d : %s - ^3RUNNING^7", testCounter, name))

    local ok, err = pcall(cb)
    if ok then
        print(string.format("^7Test %d : %s - ^2PASSED^7", testCounter, name))
        return
    end

    print(string.format("^7Assertion ^1FAILED^7 in test '%s'. %s", name, tostring(err)))
    error(err)
end

ChiliadDB.ready(function()
    ChiliadDB.dropCollection('chiliad_tester')
    ChiliadDB.dropCollection('sdfgsdfgsdf')
    ChiliadDB.dropCollection('table_tester')
    ChiliadDB.dropCollection('test44')
    ChiliadDB.dropCollection('multi_tester_renamed')
    ChiliadDB.dropCollection('ops_testing')
    ChiliadDB.dropCollection('sort_tester')
    ChiliadDB.dropCollection('coverage_tester')

    local id1 = ChiliadDB.insertOne({ collection = 'chiliad_tester', document = { name = "test1", age = 10 }, options = { selfInsertId = 'testId' } })
    local id2 = ChiliadDB.insertOne({ collection = 'chiliad_tester', document = { name = "test2", age = 10 }, options = { selfInsertId = 'testId' } })
    local id3 = ChiliadDB.insertOne({ collection = 'chiliad_tester', document = { name = "test70", age = 10 }, options = { selfInsertId = 'testId' } })
    ChiliadDB.update({ collection = 'chiliad_tester', query = { name = "test1" }, update = { name = "test69" } })
    ChiliadDB.update({ collection = 'chiliad_tester', query = { id = id2 }, update = { name = "test70", age = 11 } })
    local results3 = ChiliadDB.find({ collection = 'chiliad_tester', query = { testId = id1 } })
    local results3count = getTotalRecordsCount(results3)
    test(results3count, 1, 'insert', 'toEqual')

    local results2 = ChiliadDB.findOne({ collection = 'chiliad_tester', query = { name = "test69" } })
    test(results2.name, "test69", 'findOne', 'toEqual')

    local results5 = ChiliadDB.find({ collection = 'chiliad_tester', query = { name = "test69" } })
    local results5count = getTotalRecordsCount(results5)
    test(results5count, 1, 'find 2', 'toEqual')

    local results6 = ChiliadDB.exists({ collection = 'chiliad_tester', query = { name = "test69" } })
    test(results6, true, 'exists', 'toEqual')

    local results7 = ChiliadDB.exists({ collection = 'chiliad_tester', query = { name = "AWEFAWEFAWEFAWEFAWSEF" } })
    test(results7, false, 'exists 2', 'toEqual')

    local results8 = ChiliadDB.find({ collection = 'chiliad_tester', query = { name = "test69" } })
    test(#results8, 1, 'find 3', 'toEqual')

    local results9 = ChiliadDB.find({ collection = 'chiliad_tester', query = { name = "test70" } })
    test(getTotalRecordsCount(results9), 2, 'find 4', 'toEqual')

    local results10 = ChiliadDB.find({ collection = 'chiliad_tester', query = { name = "test70" }, options = { limit = 2 } })
    test(getTotalRecordsCount(results10), 2, 'find with limit', 'toEqual')
    test(results10[2].name, "test70", 'find with limit', 'toEqual')
    test(results10[2].age, 11, 'find with limit', 'toEqual')
    test(results10[1], nil, 'find with limit', 'toEqual')

    local results11 = ChiliadDB.find({ collection = 'chiliad_tester', query = { name = "test70" }, options = { excludeIndexes = true } })
    test(#results11, 2, 'find with excludeIndexes', 'toEqual')

    local currentIndexCheck = 3
    local result10 = ChiliadDB.getCollectionProperties('chiliad_tester')
    test(result10.currentIndex, currentIndexCheck, 'getCurrentTableIndex', 'toEqual')

    ChiliadDB.insertOne({ collection = 'chiliad_tester', document = { name = "asdf", age = 40 } })
    local result11 = ChiliadDB.getCollectionProperties('chiliad_tester')
    test(result11.currentIndex, currentIndexCheck + 1, 'getCurrentTableIndex 2', 'toEqual')

    runAssertTest('find all returns keyed records', function()
        local results12 = ChiliadDB.find({ collection = 'chiliad_tester' })
        assert(#results12 == 4)
        assert(results12[1].testId == 1)
    end)

    ChiliadDB.delete({ collection = 'chiliad_tester', query = { id = id1 } })
    local results13 = ChiliadDB.find({ collection = 'chiliad_tester' })
    test(getTotalRecordsCount(results13), 3, 'find 6', 'toEqual')
    test(ChiliadDB.getCollectionDocumentCount('chiliad_tester'), 3, 'find 6', 'toEqual')

    test(ChiliadDB.find({ table = 'asdf', options = { limit = 1 } }), false, 'Error Handling', 'toEqual')
    test(ChiliadDB.replaceOne({ collection = "character_skills" }), false, 'Error Handling', 'toEqual')
    test(ChiliadDB.insertOne({ collection = "character_skills" }), false, 'Error Handling', 'toEqual')
    test(ChiliadDB.update({ collection = "character_skills" }), false, 'Error Handling', 'toEqual')

    local results14 = ChiliadDB.find({ collection = 'chiliad_tester', options = { excludeFields = { name = true }, limit = 1 } })
    test(getTotalRecordsCount(results14), 1, 'find with limit 2', 'toEqual')

    local resultLt = ChiliadDB.exists({ collection = 'chiliad_tester', query = { age = { ['$lt'] = '11' } } }) -- this should return false since 11 is a string
    test(resultLt, false, 'exists with lt datatype mis-match', 'toEqual')

    local resultLt2 = ChiliadDB.exists({ collection = 'chiliad_tester', query = { age = { ['$lt'] = 11 } } }) -- this should return false since 11 is a string
    test(resultLt2, true, 'exists with lt datatype match', 'toEqual')

    local results19 = ChiliadDB.findOne({ collection = 'chiliad_tester', query = { age = 10 }, options = { excludeFields = { name = true, lastUpdated = true, testId = true } } })
    test(results19.name, nil, 'excludeFields working', 'toEqual')
    test(results19.lastUpdated, nil, 'excludeFields working', 'toEqual')
    test(results19.testId, nil, 'excludeFields working', 'toEqual')
    test(results19.age, 10, 'excludeFields working', 'toEqual')

    local results20 = ChiliadDB.getCollectionProperties('asdfasdf')
    test(results20, false, 'getCollectionProperties for non-existent collection', 'toEqual')

    runAssertTest('getCollectionProperties for valid collection', function()
        local results21 = ChiliadDB.getCollectionProperties('chiliad_tester')
        assert(results21.currentIndex == 4)
    end)

    runAssertTest('insert many and check output', function()
        local insertManyIds = ChiliadDB.insert({
            collection = 'chiliad_tester',
            documents = {
                { name = "test70", age = 10 },
                { name = "test70", age = 10 },
                { name = "test70", age = 11 },
                { name = "test70", age = 10 },
                { name = "test70", age = 10 },
                { name = "test70", age = 10 },
                { name = "test70", age = 10 },
            },
            options = { selfInsertId = 'testId' }
        })
        ChiliadDB.delete({ collection = 'chiliad_tester', query = { id = insertManyIds[5] } })
        ChiliadDB.delete({ collection = 'chiliad_tester', query = { id = insertManyIds[6] } })
        ChiliadDB.delete({ collection = 'chiliad_tester', query = { id = insertManyIds[7] } })
        ChiliadDB.insertOne({ collection = 'chiliad_tester', document = { name = "test70", age = 10 }, options = { selfInsertId = 'testId' } })
        ChiliadDB.insertOne({ collection = 'chiliad_tester', document = { name = "test70", age = 10 }, options = { selfInsertId = 'testId' } })

        local results22 = ChiliadDB.find({ collection = 'chiliad_tester', query = { name = "test70" } })
        local total = getTotalRecordsCount(results22)
        assert(total == 8)
    end)

    runAssertTest('skipIfExists insertOne', function()
        ChiliadDB.insertOne({ collection = 'chiliad_tester', document = { name = "testexists" }, options = { skipIfExists = { name = true } } })
        ChiliadDB.insertOne({ collection = 'chiliad_tester', document = { name = "testexists" }, options = { skipIfExists = { name = true } } })
        local results23 = ChiliadDB.find({ collection = 'chiliad_tester', query = { name = "testexists" } })
        local total23 = getTotalRecordsCount(results23)
        assert(total23 == 1)
    end)

    runAssertTest('includeFields find', function()
        local results24 = ChiliadDB.find({ collection = 'chiliad_tester', options = { includeFields = { name = true, age = true } } })
        assert(results24[7].name ~= nil)
        assert(results24[7].age ~= nil)
        assert(results24[7].age == 11)
        assert(results24[7].testId == nil)
    end)

    runAssertTest('excludeFields find', function()
        local results25 = ChiliadDB.find({ collection = 'chiliad_tester', options = { excludeFields = { name = true, age = true } } })
        assert(results25[12].name == nil)
        assert(results25[12].age == nil)
        assert(results25[12].testId ~= nil)
    end)

    runAssertTest('excludeFields find with limit', function()
        local results26 = ChiliadDB.find({ collection = 'chiliad_tester', options = { excludeFields = { age = true }, limit = 8 } })
        local count = getTotalRecordsCount(results26)
        assert(count == 8)
        assert(results26[7].testId == 7)
        assert(results26[7].age == nil)
        assert(results26[13] == nil)
    end)

    runAssertTest('delete many', function()
        local results271 = ChiliadDB.find({ collection = 'chiliad_tester', query = { name = "testnew" }, options = { excludeIndexes = true } })
        assert(#results271 == 0)
        local results272 = ChiliadDB.find({ collection = 'chiliad_tester' })
        local results272Count = getTotalRecordsCount(results272)
        assert(results272Count == 10)
        ChiliadDB.update({ collection = 'chiliad_tester', query = { testId = 5 }, update = { name = "testnew" } })
        ChiliadDB.update({ collection = 'chiliad_tester', query = { testId = 6 }, update = { name = "testnew" } })
        local results273 = ChiliadDB.find({ collection = 'chiliad_tester', query = { name = "testnew" }, options = { excludeIndexes = true } })
        assert(#results273 == 2)
        ChiliadDB.delete({ collection = 'chiliad_tester', query = { name = "testnew" } })
        results273 = ChiliadDB.find({ collection = 'chiliad_tester', query = { name = "testnew" }, options = { excludeIndexes = true } })
        assert(#results273 == 0)
        local results274 = ChiliadDB.find({ collection = 'chiliad_tester' })
        local results274Count = getTotalRecordsCount(results274)
        assert(results274Count == 8)
    end)

    runAssertTest('upsert insert', function()
        local upsetInsert = ChiliadDB.update({ collection = 'chiliad_tester', query = { name = "test420420420" }, update = { name = "testupsert" }, options = { upsert = true, selfInsertId = 'testId' } })
        local upsetInsertId = upsetInsert[1]
        assert(upsetInsertId == 15)
    end)

    runAssertTest('upsert update creates collection and returns matched ids', function()
        local upsetIdArray = ChiliadDB.update({ collection = 'sdfgsdfgsdf', query = { name = "test420420420", asdf = 'Meghan' }, update = { name = "testupsert" }, options = { upsert = true, selfInsertId = 'testId' } })
        local upsetId = upsetIdArray[1]
        assert(upsetId == 1)

        local upsetIdArray2 = ChiliadDB.update({ collection = 'sdfgsdfgsdf', query = { name = "testYEAH" }, update = { name = "testupsert" }, options = { upsert = true, selfInsertId = 'testId' } })
        local upsetId2 = upsetIdArray2[1]
        assert(upsetId2 == 2)

        local upsetIdArray3 = ChiliadDB.update({ collection = 'sdfgsdfgsdf', query = { name = "test420420420", asdf = 'Meghan' }, update = { name = "testupsert2" }, options = { upsert = true, selfInsertId = 'testId' } })
        local upsetId3 = upsetIdArray3[1]
        assert(upsetId3 == 1)
        assert(#upsetIdArray3 == 1)
    end)

    runAssertTest('includeFields findOne', function()
        local results30 = ChiliadDB.findOne({ collection = 'chiliad_tester', query = { name = "test420420420" }, options = { includeFields = { name = true } } })
        assert(results30.name ~= nil)
        assert(results30.age == nil)
        assert(results30.testId == nil)
    end)

    runAssertTest('replaceOne', function()
        local replaceOneId = ChiliadDB.replaceOne({ collection = 'sdfgsdfgsdf', query = { name = "testupsert2", asdf = 'Meghan' }, document = { name2 = 'Tater', name3 = nil } })
        local results31 = ChiliadDB.find({ collection = 'sdfgsdfgsdf' })
        assert(results31[1].name2 == 'Tater')
        assert(results31[1].name == nil)
        assert(replaceOneId == 1)
    end)

    runAssertTest('set key to empty table', function()
        local results32 = ChiliadDB.findOne({ collection = 'chiliad_tester', query = { testId = 15 } })
        assert(results32.stuff == nil)
        ChiliadDB.update({ collection = 'chiliad_tester', query = { testId = 15 }, update = { stuff = {} } })
        local results322, results322Id = ChiliadDB.findOne({ collection = 'chiliad_tester', query = { testId = 15 } })
        assert(results322.stuff ~= nil)
        assert(type(results322.stuff) == 'table')
        assert(#results322.stuff == 0)
        assert(results322Id == 15)
    end)

    runAssertTest('find with exists query operator', function()
        local result33 = ChiliadDB.find({ collection = 'chiliad_tester', query = { testId = { ['$exists'] = false } } })
        assert(result33[4].name ~= nil)
    end)

    runAssertTest('findOne by id', function()
        local result34 = ChiliadDB.findOne({ collection = 'chiliad_tester', query = { id = 15 } })
        assert(result34.name == 'test420420420')
        local result341 = ChiliadDB.findOne({ collection = 'chiliad_tester', query = { id = 1 } })
        assert(result341 == nil)
    end)

    runAssertTest('collection retention and document count', function()
        local result35 = ChiliadDB.createCollection('test44')
        assert(result35 == true)
        local result352 = ChiliadDB.getCollectionProperties('test44')
        assert(result352.retention == nil)
        ChiliadDB.setCollectionRetention({ collection = 'test44', retention = { seconds = 5, minutes = 1 } })
        ChiliadDB.insertOne({ collection = 'test44', document = { name = "test1", age = 10 }, options = { selfInsertId = 'testId' } })
        local result353 = ChiliadDB.getCollectionProperties('test44')
        assert(result353.retention == 65000)
        local result354 = ChiliadDB.getCollectionDocumentCount('chiliad_tester')
        assert(result354 == 9)
        ChiliadDB.setCollectionRetention({ collection = 'test44', remove = true })
        result353 = ChiliadDB.getCollectionProperties('test44')
        assert(result353.retention == nil)
    end)

    runAssertTest('contains operator', function()
        local contains1Id = ChiliadDB.insertOne({ collection = 'sdfgsdfgsdf', document = { stuff = { 'Jeff', 'Kale', 'Al' } } })
        local contains2Id = ChiliadDB.insertOne({ collection = 'sdfgsdfgsdf', document = { stuff = { 'Tater', 'Ash', 'Sonny' } } })
        local result36 = ChiliadDB.find({ collection = 'sdfgsdfgsdf', query = { stuff = { ['$contains'] = 'Tater' } } })
        assert(result36[contains2Id] ~= nil)
        assert(result36[contains1Id] == nil)
    end)

    runAssertTest('in and nin operators', function()
        local joeId = ChiliadDB.insertOne({ collection = 'table_tester', document = { stuff = 'Joe' } })
        local daleId = ChiliadDB.insertOne({ collection = 'table_tester', document = { stuff = 'Dale' } })
        local bobId = ChiliadDB.insertOne({ collection = 'table_tester', document = { stuff = 'Bob' } })
        local danId = ChiliadDB.insertOne({ collection = 'table_tester', document = { stuff = 'Dan' } })

        local result37 = ChiliadDB.find({ collection = 'table_tester', query = { stuff = { ['$in'] = { 'Joe', 'Bob', 'Dale' } } } })
        local results37count = getTotalRecordsCount(result37)
        assert(results37count == 3)
        assert(result37[joeId] ~= nil)
        assert(result37[daleId] ~= nil)
        assert(result37[bobId] ~= nil)

        local result371 = ChiliadDB.find({ collection = 'table_tester', query = { stuff = { ['$nin'] = { 'Joe', 'Bob', 'Dale' } } } })
        local result371count = getTotalRecordsCount(result371)
        assert(result371count == 1)
        assert(result371[danId] ~= nil)
    end)

    runAssertTest('lua string match operator', function()
        local _, joeId = ChiliadDB.findOne({ collection = 'table_tester', query = { stuff = 'Joe' } })
        local _, bobId = ChiliadDB.findOne({ collection = 'table_tester', query = { stuff = 'Bob' } })
        local _, daleId = ChiliadDB.findOne({ collection = 'table_tester', query = { stuff = 'Dale' } })
        local _, danId = ChiliadDB.findOne({ collection = 'table_tester', query = { stuff = 'Dan' } })

        local result38 = ChiliadDB.find({ collection = 'table_tester', query = { stuff = { ['$match'] = '.*Da.*' } } })
        assert(result38[joeId] == nil)
        assert(result38[bobId] == nil)
        assert(result38[daleId] ~= nil)
        assert(result38[daleId].stuff == 'Dale')
        assert(result38[danId] ~= nil)
        assert(result38[danId].stuff == 'Dan')
        local result38count = getTotalRecordsCount(result38)
        assert(result38count == 2)
    end)

    runAssertTest('insert many returns ids', function()
        local ids = ChiliadDB.insert({
            collection = 'multi_tester',
            documents = {
                { name = "test2", age = 10, class = 'wizard' },
                { name = "test3", age = 20, class = 'warrior' }
            },
            options = { selfInsertId = 'testId' }
        })
        assert(#ids == 2)
        assert(ids[1] == 1)
        assert(ids[2] == 2)
    end)

    runAssertTest('insert many with skipIfExistsCheck', function()
        local skipIfExistsCheck = ChiliadDB.insert({
            collection = 'multi_tester',
            documents = {
                { name = "test4", age = 20, class = 'warrior' },
                { name = "test4", age = 12, class = 'warrior' }
            },
            options = { skipIfExists = { age = true } }
        })
        assert(#skipIfExistsCheck == 2)
        assert(skipIfExistsCheck[1] == false)
        assert(skipIfExistsCheck[2] == 3)
    end)

    runAssertTest('renameCollection', function()
        local renameResult = ChiliadDB.renameCollection({ collection = 'multi_tester', newName = 'multi_tester_renamed' })
        assert(renameResult == true)
        local renameFinder = ChiliadDB.find({ collection = 'multi_tester_renamed' })
        assert(#renameFinder == 3)
    end)

    runAssertTest('aggregate with group sum', function()
        ChiliadDB.insert({
            collection = 'multi_tester_renamed',
            documents = {
                { name = "test4", age = 20, class = 'wizard' },
                { name = "test4", age = 12 },
                { name = "test4", age = 30, class = 'wizard' },
                { name = "test4", age = 45, class = 'paladin' }
            }
        })
        local aggregateResult = ChiliadDB.aggregate({ collection = 'multi_tester_renamed', query = { name = "test4" }, group = { fields = { "name", "class" }, sum = "age", alias = "ageSum" } })
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
    end)

    -- local stuffs = {}
    -- for i=1, 2000 do
    --     stuffs[i] = {name = "test4", age = i, class = 'wizard'}
    -- --     ChiliadDB.insertOne({collection = 'gsdfger', document = {name = "test4", age = i, class = 'wizard'}})
    -- end
    -- ChiliadDB.insert({collection = 'gsdfger', documents = stuffs})

    -- -- find all documents in the collection gsdfger where age is greater than 1000 and less than 2000
    -- local result43 = ChiliadDB.find({collection = 'gsdfger', query = {age = {['$gt'] = 1000, ['$lt'] = 3002}}})
    -- print(json.encode(result43, {indent = true}))

    -- -- find all documents where age is divisible by 100
    -- local result44 = ChiliadDB.find({collection = 'gsdfger', query = {age = {['$mod'] = {100, 0}}}})
    -- print(json.encode(result44[1100]))
    -- ChiliadDB.insertOne({collection = 'gsdfger', document = {name = {'asd', 'asdf2','asd66', 'asdf288'}, age = 2001, class = 'wizard'}})
    -- local result45 = ChiliadDB.find({collection = 'gsdfger', query = {name = {['$size'] = 4}}})
    -- print(json.encode(result45))
    local result43id1 = ChiliadDB.insertOne({ collection = 'ops_testing', document = { name = { 'test1' }, age = 40, class = 'wizard' } })
    local result43id2 = ChiliadDB.insertOne({ collection = 'ops_testing', document = { name = { 'test1', 'test2' }, age = 22, class = 'wizard' } })
    local result43id3 = ChiliadDB.insertOne({ collection = 'ops_testing', document = { name = { 'test1', 'test2', 'test3' }, age = 10, class = 'wizard' } })
    local result43id4 = ChiliadDB.insertOne({ collection = 'ops_testing', document = { name = { 'test1', 'test2', 'test3' }, age = 22, class = 'wizard' } })
    local result43id5 = ChiliadDB.insertOne({ collection = 'ops_testing', document = { name = { 'test1', 'test2' }, age = 22, class = 'wizard' } })
    local result43id6 = ChiliadDB.insertOne({ collection = 'ops_testing', document = { name = { 'test1', 'test2', 'test3' }, age = 22, class = 'wizard' } })
    local result43id7 = ChiliadDB.insertOne({ collection = 'ops_testing', document = { name = 'Joe', age = 22, class = 'wizard' } })

    runAssertTest('$mod operator', function()
        local result43 = ChiliadDB.find({ collection = 'ops_testing', query = { age = { ['$mod'] = { 10, 0 } } } })
        assert(result43[result43id1] ~= nil)
        assert(result43[result43id2] == nil)
        assert(result43[result43id3] ~= nil)
    end)

    runAssertTest('$size operator', function()
        local result441 = ChiliadDB.find({ collection = 'ops_testing', query = { name = { ['$size'] = 1 } } })
        assert(result441[result43id1] ~= nil)

        local result442 = ChiliadDB.find({ collection = 'ops_testing', query = { name = { ['$size'] = 3 } } })
        assert(result442[result43id3] ~= nil)
        assert(result442[result43id4] ~= nil)
        assert(result442[result43id6] ~= nil)
        assert(getTotalRecordsCount(result442) == 3)
    end)

    runAssertTest('$type operator', function()
        local result45 = ChiliadDB.find({ collection = 'ops_testing', query = { name = { ['$type'] = 'string' } } })
        assert(result45[result43id7] ~= nil)
        assert(result45[result43id7].name == 'Joe')
        assert(getTotalRecordsCount(result45) == 1)
    end)

    runAssertTest('sort with limit and excludeIndexes', function()
        ChiliadDB.insert({
            collection = 'sort_tester',
            documents = {
                { name = 'mid',      score = 50 },
                { name = 'high',     score = 100 },
                { name = 'low',      score = 10 },
                { name = 'upperMid', score = 75 }
            }
        })

        local sorted = ChiliadDB.find({
            collection = 'sort_tester',
            options = {
                sort = { field = 'score', order = 'desc' },
                limit = 2,
                excludeIndexes = true
            }
        })

        assert(#sorted == 2)
        assert(sorted[1].name == 'high')
        assert(sorted[2].name == 'upperMid')
    end)

    runAssertTest('updateOne by query and by id', function()
        local firstUpdatedId = ChiliadDB.updateOne({
            collection = 'sort_tester',
            query = { name = 'mid' },
            update = { score = 55, label = 'updated' }
        })
        assert(firstUpdatedId ~= false)
        local firstUpdatedDoc = ChiliadDB.findOne({ collection = 'sort_tester', query = { id = firstUpdatedId } })
        assert(firstUpdatedDoc.score == 55)
        assert(firstUpdatedDoc.label == 'updated')

        local byIdResult = ChiliadDB.updateOne({
            collection = 'sort_tester',
            query = { id = firstUpdatedId },
            update = { score = 56 }
        })
        assert(byIdResult == firstUpdatedId)
        local byIdDoc = ChiliadDB.findOne({ collection = 'sort_tester', query = { id = firstUpdatedId } })
        assert(byIdDoc.score == 56)
    end)

    runAssertTest('deleteOne by query and by id', function()
        local deletedByQueryId = ChiliadDB.deleteOne({
            collection = 'sort_tester',
            query = { name = 'low' }
        })
        assert(deletedByQueryId ~= false)
        assert(ChiliadDB.findOne({ collection = 'sort_tester', query = { id = deletedByQueryId } }) == nil)

        local extraId = ChiliadDB.insertOne({ collection = 'sort_tester', document = { name = 'temporary', score = 5 } })
        local deletedById = ChiliadDB.deleteOne({ collection = 'sort_tester', query = { id = extraId } })
        assert(deletedById == extraId)
        assert(ChiliadDB.findOne({ collection = 'sort_tester', query = { id = extraId } }) == nil)
    end)

    runAssertTest('count and distinct', function()
        local totalCount = ChiliadDB.count({ collection = 'sort_tester' })
        assert(totalCount == 3)

        local aboveFiftyCount = ChiliadDB.count({ collection = 'sort_tester', query = { score = { ['$gte'] = 50 } } })
        assert(aboveFiftyCount == 3)

        ChiliadDB.insertOne({ collection = 'coverage_tester', document = { role = 'admin', active = true } })
        ChiliadDB.insertOne({ collection = 'coverage_tester', document = { role = 'mod', active = true } })
        ChiliadDB.insertOne({ collection = 'coverage_tester', document = { role = 'admin', active = false } })

        local distinctAll = ChiliadDB.distinct({ collection = 'coverage_tester', field = 'role' })
        assert(#distinctAll == 2)
        local distinctActive = ChiliadDB.distinct({ collection = 'coverage_tester', field = 'role', query = { active = true } })
        assert(#distinctActive == 2)
    end)

    runAssertTest('touch updates lastUpdated', function()
        local touchId = ChiliadDB.insertOne({ collection = 'coverage_tester', document = { role = 'touched' } })
        local beforeTouch = ChiliadDB.findOne({ collection = 'coverage_tester', query = { id = touchId } })
        Wait(5)
        local touchResult = ChiliadDB.touch({ collection = 'coverage_tester', query = { id = touchId } })
        assert(touchResult[1] == touchId)
        local afterTouch = ChiliadDB.findOne({ collection = 'coverage_tester', query = { id = touchId } })
        assert(afterTouch.lastUpdated >= beforeTouch.lastUpdated)
    end)

    runAssertTest('collectionExists and duplicate createCollection handling', function()
        assert(ChiliadDB.collectionExists('sort_tester') == true)
        assert(ChiliadDB.collectionExists('does_not_exist') == false)
        assert(ChiliadDB.createCollection('sort_tester') == false)
    end)
end)
