Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt init
- dbt debug 
- dbt seed
- dbt snapshot
- dbt source freshness
- dbt docs generate
- dbt docs serve
- dbt clean
- dbt compile 
- dbt run
- dbt test
- dbt build (dbt run + dbt test + dbt seed + dbt snapshot) 
- dbt retry

### Flags
- dbt --help or -h 
- dbt --version or -v
- dbt run --full-refresh
- dbt run --fail-fast
- dbt run -t prod (overwrite target='dev')
- dbt run --select int_trips_unioned or dbt run --select +int_trips_unioned+ upstream/downstream
- dbt run --select tag: or state: or path 


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
