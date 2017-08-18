// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.journal;

import com.baidu.palo.common.io.Writable;

import java.util.List;

public interface Journal {
    
    // Open the journal environment
    public void open();
    
    // Roll Edit file or database
    public void rollJournal();
    
    // Get the newest journal id 
    public long getMaxJournalId();
    
    // Get the oldest journal id
    public long getMinJournalId();
    
    // Close the environment
    public void close();
    
    // Get the journal which id = journalId
    public JournalEntity read(long journalId);
    
    // Get all the journals whose id: fromKey <= id <= toKey
    // toKey = -1 means toKey = Long.Max_Value
    public JournalCursor read(long fromKey, long toKey);
    
    // Write a journal and sync to disk
    public void write(short op, Writable writable);
    
    // Delete journals whose max id is less than deleteToJournalId
    public void deleteJournals(long deleteJournalToId);
    
    // Current db's min journal id - 1
    public long getFinalizedJournalId();
    
    // Get all the dbs' name
    public List<Long> getDatabaseNames();
    
}
