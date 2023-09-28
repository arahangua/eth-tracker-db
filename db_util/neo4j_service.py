from neo4j import GraphDatabase
from py2neo import Graph # a bit better syntax compared to neo4j module (for cypher query)
import pandas as pd

class Neo4jService:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

        # Connect to the graph database (py2neo)
        self._graph = Graph(uri, auth=(user, password))

    # Create nodes (py2neo)
    def create_node_dict(self, node_alias, prop_name, prop_val):
        new_node = {"label": node_alias, "property": prop_name, "value": prop_val}
        return new_node

    # # Create a relationship between nodes (py2neo)
    def _create_relationship(self, node1, rel, node2):
        query = (
            f"MERGE (a:{node1['label']} {{{node1['property']}: '{node1['value']}'}})"
            f"MERGE (b:{node2['label']} {{{node2['property']}: '{node2['value']}'}})"
            f"MERGE (a)-[:{rel.upper()} {{weight:1}}]->(b)"
        )
        
        self._graph.run(query)

    
    # Add nodes and relationship to the graph by checking pre-existing relationships (py2neo)
    def create_relationship(self, node1, rel, node2):

        if(self.relationship_exists(node1, rel, node2)):
            self._update_relationship(node1, rel, node2)
        else:     
            self._create_relationship(node1, rel, node2)

    # format : start_node = {"label": "Person", "property": "name", "value": "Alice"}
    def relationship_exists(self, node1, rel, node2):
        query = (
            f"MATCH (a:{node1['label']} {{{node1['property']}: '{node1['value']}'}})"
            f"-[r:{rel.upper()}]->"
            f"(b:{node2['label']} {{{node2['property']}: '{node2['value']}'}})"
            " RETURN r"
        )
        
        results = self._graph.run(query).data()
        return len(results) > 0
    
    def _update_relationship(self, node1, rel, node2):
        query = (
            f"MATCH (a:{node1['label']} {{{node1['property']}: '{node1['value']}'}})"
            f"-[r:{rel.upper()}]->"
            f"(b:{node2['label']} {{{node2['property']}: '{node2['value']}'}})"
            f"SET r.weight = COALESCE(r.weight, 1) + 1 RETURN r.weight"
        )

        self._graph.run(query)

    def delete_relationship(self, node1, rel, node2):
        query = (
            f"MATCH (a:{node1['label']} {{{node1['property']}: '{node1['value']}'}})"
            f"-[r:{rel.upper()}]->"
            f"(b:{node2['label']} {{{node2['property']}: '{node2['value']}'}})"
            f"DELETE r"
        )
        self._graph.run(query)

    def delete_node(self, node1):
        query = (
            f"MATCH (a:{node1['label']} {{{node1['property']}: '{node1['value']}'}})"
            f"DETACH DELETE a"
        )
        self._graph.run(query)

    def create_node(self, node1):
        query = (
            f"CREATE (a:{node1['label']} {{{node1['property']}: '{node1['value']}'}})"
        )
        self._graph.run(query)

    def set_unique_rule(self):
         # set unique constraint
        query = (
            "CREATE CONSTRAINT FOR (p:address) REQUIRE p.hash IS UNIQUE"

        )
        self._graph.run(query)

    def batch_create_from_df(self, pd_df, source_alias, dest_alias, rel_alias, mount_fol):
        rels = pd.unique(pd_df[rel_alias])

       
        # neo4j cannot handle multiple relations for a batch job.
        for rel in rels:
            rel_df = pd_df[pd_df[rel_alias]==rel]
            rel_df.to_csv(f'../{mount_fol}/import/tmp.csv', index=False)
            
            # first write csv file 
            source_nodes = rel_df.drop_duplicates(subset=source_alias)
            dest_nodes = rel_df.drop_duplicates(subset=dest_alias)
            rel_nodes = pd.concat([source_nodes, dest_nodes])
            rel_nodes.to_csv(f'../{mount_fol}/import/tmp_node.csv', index=False)
            # # self node
            # query = (
            #     f"MERGE (a:address {{hash: '{name}'}}) "
            # )
            # self._graph.run(query)

            # other nodes
            query = (
                f"LOAD CSV WITH HEADERS FROM 'file:///tmp_node.csv' AS row "

                # Match or create the 'b' node based on the CSV file
                f"MERGE (b:address {{hash: row[\'address\']}}) "
            )
            self._graph.run(query)
            
            for s_node in source_nodes:
                query = (
                    f"MATCH (a:address  {{hash: '{s_node}'}}) "
                    # Load the CSV file
                    f"LOAD CSV WITH HEADERS FROM 'file:///tmp.csv' AS row "

                    # Match or create the 'b' node based on the CSV file
                    f"MATCH (b:address {{hash: row[\'{dest_alias}\']}}) "

                    # Merge the relationship and set the weight
                    f"MERGE (a)-[r:{rel.upper()}]->(b) "
                    f"ON CREATE SET r.weight = 1 "
                    f"ON MATCH SET r.weight = r.weight + 1 "
                )
                self._graph.run(query)
        print(f'batch import job done')
    
    
    def tag_names(self, name_df):
        for i, row in name_df.iterrows():
            query = (
                f"MATCH (a:address {{hash: '{row['address']}'}})"
                f"SET a.name = '{row['name']}'"
            )
            self._graph.run(query)
        print('setting name done')

    def purge(self):
        query = (
            f"MATCH (n) DETACH DELETE n"
        )
        self._graph.run(query)

    # neo4j doesn't allow batch update for multiple relations always need to run a job for one relation type.
    #
    def add_int_prop_from_df(self, name, pd_df, rel_alias, prop_name, mount_fol):
        rels = pd.unique(pd_df[rel_alias])

        for rel in rels:
            rel_df = pd_df[pd_df[rel_alias]==rel]
            rel_df.to_csv(f'../{mount_fol}/import/tmp.csv', index=False)
            
            query = (
                    f"LOAD CSV WITH HEADERS FROM 'file:///tmp.csv' AS row "

                    f"MATCH (a:address {{hash: row[\'from\']}})-[r:{rel.upper()}]->(b:address {{hash: row[\'to\']}})"
                    f"SET r.{prop_name} = coalesce(r.{prop_name}, []) + toInteger(row[\'{prop_name}\'])"
                )
            self._graph.run(query)
        print(f'adding blocknumbers for {name}.csv done.')

    def add_blnum_txindex_from_df(self, name, pd_df, rel_alias, mount_fol):
        rels = pd.unique(pd_df[rel_alias])

        for rel in rels:
            rel_df = pd_df[pd_df[rel_alias]==rel]
            rel_df.to_csv(f'../{mount_fol}/import/tmp.csv', index=False)
            
            query = (
                    f"LOAD CSV WITH HEADERS FROM 'file:///tmp.csv' AS row "

                    f"MATCH (a:address {{hash: '{name}'}})-[r:{rel.upper()}]->(b:address {{hash: row[\'address\']}})"
                    f"SET r.blockNumber = coalesce(r.blockNumber, []) + toInteger(row[\'blockNumber\'])"
                    f"SET r.tx_index = coalesce(r.tx_index, []) + toInteger(row[\'transactionPosition\'])"
                )
            self._graph.run(query)
        print(f'adding blocknumbers for {name}.csv done.')

    def close(self):
        self._driver.close()


    def check_connection(self):
        try:
            # Try to create a session and run a simple query
            with self._driver.session() as session:
                session.run("RETURN 1")
            print("Successfully connected to the Neo4j database!")
        except Exception as e:
            print(f"Failed to connect to the Neo4j database. Error: {e}")
        finally:
            self._driver.close()

    def retrieve_data(self, query):
        results = self._graph.run(query).data()
        df = pd.DataFrame(results)
        return df

###############################################################################################
############# below are task specific ##########################################################

    def batch_merge_trace_filter_csv(self, csv_file, batch_size=1000):
        query = (
            f'''
            CALL {{
            LOAD CSV WITH HEADERS FROM 'file:///{csv_file}' AS row 
            MERGE (fromNode:address {{hash: row[\'from\']}}) 
            MERGE (toNode:address {{hash: row[\'to\']}}) 
            MERGE (fromNode)-[r:tx {{type: row[\'decoded\']}}]-> (toNode) 
            ON CREATE 
                SET r.weight = 1, 
                    r.tx_locs = [toInteger(row[\'blockNumber\'] + row[\'transactionPosition\'])], 
                    r.blockNumber = [toInteger(row[\'blockNumber\'])], 
                    r.tx_pos = [toInteger(row[\'transactionPosition\'])] 
            ON MATCH 
                SET r.weight = CASE 
                        WHEN NOT (toInteger(row['blockNumber']) IN r.blockNumber AND toInteger(row['transactionPosition']) IN r.tx_pos) 
                        THEN COALESCE(r.weight, 0) + 1 
                        ELSE r.weight 
                   END,
                    r.tx_locs = CASE 
                       WHEN NOT (toInteger(row['blockNumber']) IN r.blockNumber AND toInteger(row['transactionPosition']) IN r.tx_pos) 
                       THEN coalesce(r.tx_locs, []) + [toInteger(row['blockNumber'] + row['transactionPosition'])] 
                       ELSE r.tx_locs 
                    END,
                    r.blockNumber = CASE 
                        WHEN NOT (toInteger(row['blockNumber']) IN r.blockNumber AND toInteger(row['transactionPosition']) IN r.tx_pos) 
                        THEN coalesce(r.blockNumber, []) + [toInteger(row['blockNumber'])] 
                        ELSE r.blockNumber 
                    END,
                    r.tx_pos = CASE 
                        WHEN NOT (toInteger(row['blockNumber']) IN r.blockNumber AND toInteger(row['transactionPosition']) IN r.tx_pos) 
                        THEN coalesce(r.tx_pos, []) + [toInteger(row['transactionPosition'])] 
                        ELSE r.tx_pos 
                   END
            }} IN TRANSACTIONS OF {batch_size} ROWS
            '''
            )

        self._graph.run(query)
        print(f'batch merge job for {csv_file} done.')
    
    
    def batch_merge_transfer_csv(self, csv_file, batch_size=1000):
        query = (
            f'''
            CALL {{
            LOAD CSV WITH HEADERS FROM 'file:///{csv_file}' AS row 
            MERGE (fromNode:address {{hash: row['from']}}) 
            MERGE (toNode:address {{hash: row['to']}}) 
            MERGE (fromNode)-[r:transfer {{type: row['function']}}]-> (toNode) 
            ON CREATE 
                SET r.weight = 1, 
                    r.symbol = row['symbol'],
                    r.decimal = row['decimal'],
                    r.token_addr = row['token_addr'],
                    r.value = [toFloat(row['value'])],
                    r.blocknumber = [toInteger(row['blocknumber'])], 
                    r.tx_pos = [toInteger(row['tx_pos'])],
                    r.transfer_seq = [toInteger(row['transfer_seq'])]
            ON MATCH 
                SET r.weight = COALESCE(r.weight, 0) + 1, 
                    r.value =  COALESCE(r.value, []) + [toFloat(row['value'])],
                    r.blocknumber = COALESCE(r.blocknumber, []) + [toInteger(row['blocknumber'])],
                    r.tx_pos = COALESCE(r.tx_pos, []) + [toInteger(row['tx_pos'])],
                    r.transfer_seq = COALESCE(r.transfer_seq, []) + [toInteger(row['transfer_seq'])]
            }} IN TRANSACTIONS OF {batch_size} ROWS
            '''
            )

        self._graph.run(query)
        print(f'batch merge job for {csv_file} done.')





    # export the graph as graphml file for gephi visualization
    def export_to_graphml(self, alias):
        query = (
        f'''
        CALL apoc.export.graphml.query(
        "MATCH (a)-[r]->(b) RETURN a,r,b",
        "{alias}.graphml",
        {{
            useTypes: true,
            storeNodeIds: false,
            defaultRelationshipType: "UNKNOWN",
            readLabels: true,
            labels: ["hash"],
            relTypes: ["type"],
            caption: ["hash"],
            arrayStyle: true,
            stringIds: false
        }}
        )
        '''
        )
        self._graph.run(query)
        print('exported to graphml')


    def export_n_hop_neighborhood(self, save_name, target_addr, n_hop=2, to='graphml'):
        if(to =='graphml'):
            query=(
            f'''
            CALL apoc.export.graphml.query(
            "MATCH path = (s:address {{hash: '{target_addr}'}})-[*1..{n_hop}]-(neighbor) RETURN path",
            "{save_name}.graphml",
            {{format: 'graphml'}})
            '''
            )

        elif(to=='csv'):
            query = (
                f'''
                CALL apoc.export.csv.query(
                "MATCH path = (s:address {{hash: '{target_addr}'}})-[*1..{n_hop}]-(neighbor) 
                UNWIND relationships(path) AS rel
                RETURN startNode(rel).hash as SourceNode, endNode(rel).hash as TargetNode, rel.type as RelationshipType",
                "{save_name}.csv",
                {{}})
                '''
            )
        
        self._graph.run(query)
        print('done exporting n-hop neighborhood')

    def export_custom_query(self, save_name, custom_query, to='graphml'):
        if(to=='graphml'):
            query = (
                f'''
                CALL apoc.export.graphml.query(
                "{custom_query}",
                "{save_name}.graphml",
                {{format:'graphml'}})
                '''
                )
            
        elif(to=='csv'):
            query = (
                f'''
                CALL apoc.export.csv.query(
                "{custom_query}",
                "{save_name}.csv",
                {{}})
                '''
            )
        
        self._graph.run(query)
        print('done running the custom query')