#!/usr/bin/env ruby

# NOTE: You must run "gem install solr-ruby" for this to work.
#

require 'rubygems'
require 'solr'

# connect to the solr instance
conn = Solr::Connection.new('http://localhost:8098/solr/test', :autocommit => :off)

# add a document to the index
conn.add(:id => 123, :name => 'Lucene in Action')

# # update the document
conn.update(:id => 123, :name => 'Solr in Action')

# print out the first hit in a query for 'action'
response = conn.query('action')
print response.hits[0]

# iterate through all the hits for 'action'
conn.query('action') do |hit|
  p hit
end

delete document by id
conn.delete(123)
