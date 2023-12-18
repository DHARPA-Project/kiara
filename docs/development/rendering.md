# Render an internal type

*kiara* has a sort of loose framework that helps when there is a requirement to render an internal kiara
model instance into some external format. This has developed in a sort of ad-hoc manner, because requirements where this is the solution to where not very clearly (or at all) defined initially, the continued to surface until at some stage I got tired of implementing the same thing over and over again. At the same token, none of this seemed to ever be exposed to higher levels, which is why I never bothered to fix its API etc.

## cli usage

To see what sort of functionality is possible with this, explore the `kiara render` command. Which, up to a point should be self-exploratory.
