use scraper::{Html, Selector};

pub async fn get_json(url: &str) -> Result<String, reqwest::Error> {

    println!("Getting the json");

    let res = reqwest::get(url).await?;
    println!("Status: {}", res.status());

    let body = res.text().await?;

    let document = Html::parse_document(&body);
    
    //<script type="application/json" id="store">
    let selector = Selector::parse(r#"script[id="store"#).unwrap();
    let fragment = document.select(&selector).next().unwrap();
    
    Ok(fragment.inner_html())
}