#Copyright (c) 2016, UrbanSim Inc. All rights reserved.

#Redistribution and use in source and binary forms, with or without
#modification, are permitted provided that the following conditions are met:

#1. Redistributions of source code must retain the above copyright notice, this
#list of conditions and the following disclaimer.

#2. Redistributions in binary form must reproduce the above copyright notice,
#this list of conditions and the following disclaimer in the documentation
#and/or other materials provided with the distribution.

#3. Neither the name of the copyright holder nor the names of its contributors
#may be used to endorse or promote products derived from this software without
#specific prior written permission.

#THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
#ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
#WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
#FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
#DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
#SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
#CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
#OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import census 
import pandas as pd
import numpy as np
import us


# TODO DOCSTRING!!
class Census:

    def __init__(self, key):
        self.c = census.Census(key)
        self.base_url = "https://s3-us-west-2.amazonaws.com/synthpop-data/"
        self.pums_relationship_file_url = self.base_url + "tract10_to_puma.csv"
        self.pums_relationship_df = None
        self.pums10_population_base_url = \
            r"data\PUMs2014\puma10_p_%s_%s.csv"
        self.pums10_household_base_url = \
            r"data\PUMs2014\puma10_h_%s_%s.csv"
        self.pums00_population_base_url = \
            r"data\PUMs2014\puma00_p_%s_%s.csv"
        self.pums00_household_base_url = \
            r"data\PUMs2014\puma00_h_%s_%s.csv"
        self.pums_population_state_base_url = \
            self.base_url + "puma_p_%s.csv"
        self.pums_household_state_base_url = \
            self.base_url + "puma_h_%s.csv"
        self.fips_url = self.base_url + "national_county.txt"
        self.fips_df = None
        self.pums_cache = {}

    # df1 is the disaggregate data frame (e.g. block groups)
    # df2 is the aggregate data frame (e.g. tracts)
    # need to scale down df2 variables to df1 level totals
    def _scale_and_merge(self, df1, tot1, df2, tot2, columns_to_scale,
                         merge_columns, suffixes):
        assert df1[tot1].sum() == df2[tot2].sum()
        df = pd.merge(df1, df2, left_on=merge_columns, right_on=merge_columns,
                      suffixes=suffixes)

        # going to scale these too so store current values
        tot2, tot1 = df[tot2], df[tot1]
        # if agg number if 0, disaggregate should be 0
        # note this is filled by fillna below
        assert np.all(tot1[tot2 == 0] == 0)

        for col in columns_to_scale:
            df[col] = df[col] / tot2 * tot1
            # round?
            df[col] = df[col].fillna(0).astype('int')
        return df

    def block_group_query(self, census_columns, state, county, tract=None,
                          year=None, id=None):
        if id is None:
            id = "*"
        return self._query(census_columns, state, county,
                           forstr="block group:%s" % id,
                           tract=tract, year=year)

    def tract_query(self, census_columns, state, county, tract=None,
                    year=None):
        if tract is None:
            tract = "*"
        return self._query(census_columns, state, county,
                           forstr="tract:%s" % tract,
                           year=year)

    def _query(self, census_columns, state, county, forstr,
               tract=None, year=None):
        print year
        c = self.c

        #state, county = self.try_fips_lookup(state, county)

        if tract is None:
            in_str = 'state:%s county:%s' % (state, county)
        else:
            in_str = 'state:%s county:%s tract:%s' % (state, county, tract)

        dfs = []

        # unfortunately the api only queries 50 columns at a time
        # leave room for a few extra id columns
        def chunks(l, n):
            """ Yield successive n-sized chunks from l.
            """
            for i in xrange(0, len(l), n):
                yield l[i:i+n]

        for census_column_batch in chunks(census_columns, 45):
            census_column_batch = list(census_column_batch)
            d = c.acs5.get(['NAME'] + census_column_batch,
                          geo={'for': forstr,
                               'in': in_str},
                          year=year)
            df = pd.DataFrame(d)
            df[census_column_batch] = df[census_column_batch].astype('int')
            dfs.append(df)

        assert len(dfs) >= 1
        df = dfs[0]
        for mdf in dfs[1:]:
            df = pd.merge(df, mdf, on="NAME", suffixes=("", "_ignore"))
            drop_cols = filter(lambda x: "_ignore" in x, df.columns)
            df = df.drop(drop_cols, axis=1)

        return df

    def block_group_and_tract_query(self, block_group_columns,
                                    tract_columns, state, county,
                                    merge_columns, block_group_size_attr,
                                    tract_size_attr, tract=None, year=None):
        df2 = self.tract_query(tract_columns, state, county, tract=tract,
                               year=year)
        df1 = self.block_group_query(block_group_columns, state, county,
                                     tract=tract, year=year)

        df = self._scale_and_merge(df1, block_group_size_attr, df2,
                                   tract_size_attr, tract_columns,
                                   merge_columns, suffixes=("", "_ignore"))
        drop_cols = filter(lambda x: "_ignore" in x, df.columns)
        df = df.drop(drop_cols, axis=1)

        return df

    def _get_pums_relationship(self):
        if self.pums_relationship_df is None:
            self.pums_relationship_df = \
                pd.read_csv(self.pums_relationship_file_url, dtype={
                    "statefp": "object",
                    "countyfp": "object",
                    "tractce": "object",
                    "puma10_id": "object",
                    "puma00_id": "object",
                })
        return self.pums_relationship_df

    def _get_fips_lookup(self):
        if self.fips_df is None:
            self.fips_df = pd.read_csv(
                self.fips_url,
                dtype={
                    "State ANSI": "object",
                    "County ANSI": "object"
                },
                index_col=["State",
                           "County Name"]
            )
            del self.fips_df["ANSI Cl"]
        return self.fips_df

    def tract_to_puma(self, state, county, tract):

        state, county = self.try_fips_lookup(state, county)

        df = self._get_pums_relationship()
        q = "statefp == '%s' and countyfp == '%s' and tractce == '%s'" % \
            (state, county, tract)
        r = df.query(q)
        return r["puma10_id"].values[0], r["puma00_id"].values[0]

    def _read_csv(self, loc):
        if loc not in self.pums_cache:
            pums_df = pd.read_csv(loc, dtype={
                "PUMA10": "object",
                "PUMA00": "object",
                "ST": "object"
            })
            pums_df = pums_df.rename(columns={
                'PUMA10': 'puma10',
                'PUMA00': 'puma00',
                'SERIALNO': 'serialno'
            })
            self.pums_cache[loc] = pums_df
        return self.pums_cache[loc]

    def download_population_pums(self, state, puma10=None, puma00=None):
        print puma10, puma00
        state = self.try_fips_lookup(state)
        if (puma10 is None) & (puma00 is None):
            return self._read_csv(self.pums_population_state_base_url % (state))
        pums = self._read_csv(self.pums10_population_base_url % (state, puma10))
        if puma00 is not None:
            pums00 = self._read_csv(self.pums00_population_base_url % (state, puma00))
            pums = pd.concat([pums, pums00], ignore_index=True)
        return pums

    def download_household_pums(self, state, puma10=None, puma00=None):
        state = self.try_fips_lookup(state)
        if (puma10 is None) & (puma00 is None):
            return self._read_csv(self.pums_household_state_base_url % (state))
        pums = self._read_csv(self.pums10_household_base_url % (state, puma10))
        if puma00 is not None:
            pums00 = self._read_csv(
                self.pums00_household_base_url % (state, puma00))
            pums = pd.concat([pums, pums00], ignore_index=True)

        # filter out gq and empty units (non-hh records)
        pums = pums[(pums.RT == 'H') & (pums.NP > 0) & (pums.TYPE == 1)]

        return pums
        

    def try_fips_lookup(self, state, county=None):
        df = self._get_fips_lookup()

        if county is None:
            try:
                return getattr(us.states, state).fips
            except:
                pass
            return state

        try:
            return df.loc[(state, county)]
        except:
            pass
        return state, county
