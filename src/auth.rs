use axum::extract::{FromRequestParts, Path, Query};
use axum::http::request::Parts;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use axum::{async_trait, Extension};
use axum_extra::extract::cookie::Cookie;
use axum_extra::extract::CookieJar;
use chrono::format::Numeric::Timestamp;
use chrono::Utc;
use jwt_simple::algorithms::{ECDSAP256KeyPairLike, ECDSAP256PublicKeyLike, ES256KeyPair};
use jwt_simple::claims::{Claims, JWTClaims, NoCustomClaims};
use jwt_simple::common::VerificationOptions;
use jwt_simple::prelude::{Deserialize, Duration, Serialize, UnixTimeStamp};
use oauth2::basic::BasicClient;
use oauth2::reqwest::async_http_client;
use oauth2::{
    AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken, PkceCodeChallenge,
    PkceCodeVerifier, RedirectUrl, Scope, TokenResponse, TokenUrl,
};
use sha256::Sha256Digest;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use totp_rs::{Algorithm, TotpUrlError, TOTP};
use tracing::{info, instrument, warn};

/// Auth flow || https://developers.google.com/identity/openid-connect/openid-connect
///
/// Step 1
///     Anti-forgery
///         add a cookie "session_id" or something to the browser,
///         the cookie value should be a random uuid
///         sign this uuid with a private key, use the signature as the anti-forgery
///         then when the client comes back we can validate without storing any state
///         except for the key, on our side.
///
/// Step 2
///     Send auth request to google
///         construct a http request with the following query params
///         URL: https://accounts.google.com/o/oauth2/v2/auth
///         response_type: This should always be ```code```
///         client_id: our google auth client_id
///         scope: this is asking for permissions.. ours should be ```openid email``` to get their email address
///         redirect_uri: Where we want them to come back to after talking to google
///         state: this is the anti-forgery data we made above
///         nonce: a random value
///         login_hint: ```sub``` -- this seems to allow it to auto populate the login field with an email
///         hd: -- this seems optional, but we can set it to a domain so it is optimized for that domain's login flow??
///
///         example:
///         https://accounts.google.com/o/oauth2/v2/auth?response_type=code&client_id=<YOUR_CLIENT_ID>&scope=openid%20email&redirect_uri=https%3A//localhost%3A3000/&state=abc&login_hint=sub&nonce=<A_NONCE>```
///         
/// Step 3
///     The user returns
///         Validate the state query param by re-signing the "client_id" cookie with our private key,
///         then comparing the state
///
/// Step 4
///     Exchange your code for an access token
///         Make a post to https://oauth2.googleapis.com/token with the following query params in the body
///         code: The code that came back when the user got redirected back
///         client_id: the google auth client_id
///         client_secret: the google auth client_secret
///         redirect_uri: one of the authorized redirect URIs we have configured in our google auth
///         grant_type: ```authorization_code```
///
///     Making this http request will return us a JWT
///     The JWT has the user info we need to allow or disallow the user's access
///     ```sub``` in the token is the guaranteed unique user ID
///         
/// now we create a layer that validates an authorization header and redirects the user to google if it doesn't work
/// then when the user comes back we get our JWT from google and set their authorization header to pass through

#[derive(Serialize, Deserialize, Debug)]
pub struct GoogleUser {
    pub id: String,
    pub email: String,
    pub verified_email: bool,
    pub picture: String,
    pub hd: String,
}

#[derive(Default, Deserialize)]
pub struct GoogleAuthenticator {
    #[serde(skip)]
    code_pairs: RwLock<HashMap<String, String>>,
    #[serde(skip)]
    jwt_cache: RwLock<HashMap<String, String>>,
    client_id: String,
    client_secret: String,
    auth_uri: String,
    token_uri: String,
    redirect_uri: String,
}

impl GoogleAuthenticator {
    fn get_client(&self) -> BasicClient {
        // TODO: clean this up by actually embedding the proper types in the struct
        BasicClient::new(
            ClientId::new(self.client_id.clone()),
            Some(ClientSecret::new(self.client_secret.clone())),
            AuthUrl::new(self.auth_uri.clone()).unwrap(),
            Some(TokenUrl::new(self.token_uri.clone()).unwrap()),
        )
        .set_redirect_uri(RedirectUrl::new(self.redirect_uri.clone()).unwrap())
    }

    #[instrument(skip(self))]
    pub async fn generate_google_auth_code(&self, email: String) -> String {
        totp_from_str(&email).unwrap().generate_current().unwrap()
    }

    async fn set_jwt_cache(&self, email: String, cookie: String) {
        self.jwt_cache.write().await.insert(email, cookie);
    }

    #[instrument(skip(self))]
    async fn get_jwt_from_code(&self, code: String, email: String) -> Result<String, String> {
        match totp_from_str(&email)
            .unwrap()
            .check_current(code.trim())
            .unwrap()
        {
            true => match self.jwt_cache.read().await.get(&email) {
                None => Err("User does not have jwt cache, please re-sign in with oauth".into()),
                Some(cookies) => Ok(cookies.clone()),
            },
            false => Err("Invalid authentication code".into()),
        }
    }

    async fn exchange_code_for_user(
        &self,
        auth_response: AuthResponse,
    ) -> Result<GoogleUser, String> {
        let client = self.get_client();

        info!("{:?}", auth_response);
        let state = CsrfToken::new(auth_response.state);
        if let Some(session_id) = self.code_pairs.read().await.get(state.secret()) {
            let verifier = PkceCodeVerifier::new(session_id.into());
            let token_response = client
                .exchange_code(AuthorizationCode::new(auth_response.code))
                .set_pkce_verifier(verifier)
                .request_async(async_http_client)
                .await
                .expect("Failed to get token from google");
            let access_token = token_response.access_token();
            let user = reqwest::Client::new()
                .get("https://www.googleapis.com/oauth2/v2/userinfo?email")
                .bearer_auth(access_token.secret())
                .send()
                .await
                .expect("Failed to request profile data")
                .json::<GoogleUser>()
                .await
                .expect("Failed to deserialize profile data");
            info!("Body: {:?}", user);
            Ok(user)
        } else {
            Err("bad login".into())
        }
    }

    async fn send_to_login(&self) -> String {
        let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
        let client = self.get_client();

        // Generate the full authorization URL.
        let (auth_url, csrf_token) = client
            .authorize_url(CsrfToken::new_random)
            // Set the desired scopes.
            .add_scope(Scope::new("openid".to_string()))
            .add_scope(Scope::new("email".to_string()))
            // Set the PKCE code challenge.
            .set_pkce_challenge(pkce_challenge)
            .url();

        self.code_pairs
            .write()
            .await
            .insert(csrf_token.secret().clone(), pkce_verifier.secret().clone());
        auth_url.to_string()
    }
}

#[instrument]
fn totp_from_str(string: &str) -> Result<TOTP, TotpUrlError> {
    let string = string.digest();

    TOTP::new(
        Algorithm::SHA1,
        6,
        1,
        30,
        string.into_bytes()[0..16].to_vec(),
    )
}

#[derive(Deserialize)]
pub struct JwtManagerBuilder {
    key_path: String,
    duration: u64,
    accepted_domains: Vec<String>,
}

impl JwtManagerBuilder {
    pub fn build(self) -> JwtManager {
        JwtManager {
            key_pair: ES256KeyPair::from_pem(&std::fs::read_to_string(&self.key_path).unwrap())
                .unwrap(),
            duration: self.duration,
            accepted_domains: self.accepted_domains,
        }
    }
}

pub struct JwtManager {
    key_pair: ES256KeyPair,
    duration: u64,
    accepted_domains: Vec<String>,
}

impl JwtManager {
    fn create_token_for_user(&self, user: GoogleUser) -> String {
        let email = &user.email.clone();
        let token = Claims::with_custom_claims(
            user,
            jwt_simple::prelude::Duration::from_mins(self.duration),
        )
        .with_subject(email);
        self.key_pair.sign(token).unwrap()
    }

    #[instrument(skip(self, jwt))]
    fn validate_jwt(&self, jwt: &str) -> Result<JWTClaims<GoogleUser>, String> {
        let verification_options = VerificationOptions {
            accept_future: false,
            time_tolerance: Some(jwt_simple::prelude::Duration::from_secs(1)),
            ..Default::default()
        };
        match self
            .key_pair
            .public_key()
            .verify_token::<GoogleUser>(jwt, Some(verification_options))
        {
            Ok(claims) => {
                if self.accepted_domains.contains(&claims.custom.hd) {
                    Ok(claims)
                } else {
                    warn!("Oauth domain not accepted");
                    Err("Not an accepted domain".into())
                }
            }
            Err(error) => {
                warn!("JWT VALIDATION ERROR {}", error.to_string());
                Err(error.to_string())
            },
        }
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for GoogleUser
where
    S: Send + Sync + std::fmt::Debug,
{
    type Rejection = Response;

    #[instrument(skip(parts, _state))]
    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        info!("in user extraction");
        let jar = CookieJar::from_headers(&parts.headers);
        if let Some(jwt) = jar.get("jwt") {
            info!("got jwt token");
            let jwt_manager = parts
                .extensions
                .get::<Arc<JwtManager>>()
                .expect("No jwt manager set up");
            match jwt_manager.validate_jwt(jwt.value()) {
                Ok(token) => {
                    info!("jwt accepted");
                    Ok(token.custom)
                },
                Err(error) => {
                    warn!("{:?}", error);
                    let google_authenticator = parts
                        .extensions
                        .get::<Arc<GoogleAuthenticator>>()
                        .expect("No google authenticator set up");

                    let auth_url = google_authenticator.send_to_login().await;

                    Err(Redirect::to(&auth_url).into_response())
                }
            }
        } else {
            let google_authenticator = parts
                .extensions
                .get::<Arc<GoogleAuthenticator>>()
                .expect("No google authenticator set up");

            warn!("no jwt found!");

            let auth_url = google_authenticator.send_to_login().await;

            Err(Redirect::to(&auth_url).into_response())
        }
    }
}

#[instrument(ret, skip(google_authenticator))]
pub async fn get_jwt_cache_from_code(
    Path((email, code)): Path<(String, String)>,
    google_authenticator: Extension<Arc<GoogleAuthenticator>>,
) -> impl IntoResponse {
    match google_authenticator
        .get_jwt_from_code(email.to_lowercase().trim().into(), code)
        .await
    {
        Ok(jwt) => {
            let mut resp = (StatusCode::OK, jwt.clone()).into_response();

            resp.headers_mut().insert(
                header::SET_COOKIE,
                HeaderValue::from_str(&jwt.to_string()).unwrap(),
            );

            resp
        }
        Err(msg) => (StatusCode::UNAUTHORIZED, msg).into_response(),
    }
}

#[instrument(ret, skip(google_authenticator))]
pub async fn auth_code(
    user: GoogleUser,
    google_authenticator: Extension<Arc<GoogleAuthenticator>>,
) -> impl IntoResponse {
    google_authenticator
        .generate_google_auth_code(user.email.to_lowercase().trim().into())
        .await
}

#[instrument(ret, skip(jwt_manager, google_authenticator, auth_response))]
pub async fn login_handler(
    auth_response: Option<Query<AuthResponse>>,
    google_authenticator: Extension<Arc<GoogleAuthenticator>>,
    jwt_manager: Extension<Arc<JwtManager>>,
) -> impl IntoResponse {
    let mut login_result = Redirect::to("/protected/code").into_response();
    if let Some(auth_response) = auth_response {
        let user = google_authenticator
            .exchange_code_for_user(auth_response.0)
            .await
            .expect("Could not validate token with google");
        let email = String::clone(&user.email).to_lowercase();
        let email = email.trim();
        let token = jwt_manager.create_token_for_user(user);
        let cookie = Cookie::new("jwt", token);

        google_authenticator
            .set_jwt_cache(email.into(), cookie.to_string())
            .await;

        login_result.headers_mut().insert(
            header::SET_COOKIE,
            HeaderValue::from_str(&cookie.to_string()).unwrap(),
        );
    }
    login_result.into_response()
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct AuthResponse {
    state: String,
    code: String,
}
